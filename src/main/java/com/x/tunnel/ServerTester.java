package com.x.tunnel;

import android.os.Handler;
import android.os.Looper;
import android.text.TextUtils;
import android.util.Log;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class ServerTester {
    private static final String TAG = "ServerTester";
    private static final int TIMEOUT_MS = 3000;
    private static final ExecutorService executor = Executors.newFixedThreadPool(4);
    private static final Handler mainHandler = new Handler(Looper.getMainLooper());
    private static volatile SSLSocketFactory insecureSslFactory;

    public static final int LATENCY_TESTING = -2;
    public static final int LATENCY_TIMEOUT = -1;

    public interface TestCallback {
        void onProgress(String profileId, int latencyMs);
        void onComplete(int totalCount, int availableCount, int failCount, int currentConnLatency);
    }

    private static SSLSocketFactory getInsecureSslFactory() {
        if (insecureSslFactory == null) {
            synchronized (ServerTester.class) {
                if (insecureSslFactory == null) {
                    try {
                        SSLContext sc = SSLContext.getInstance("TLS");
                        sc.init(null, new TrustManager[]{new X509TrustManager() {
                            @Override
                            public void checkClientTrusted(X509Certificate[] chain, String authType) {}
                            @Override
                            public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                            @Override
                            public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                        }}, new SecureRandom());
                        insecureSslFactory = sc.getSocketFactory();
                    } catch (Throwable t) {
                        Log.e(TAG, "Failed to init insecure SSL", t);
                        insecureSslFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
                    }
                }
            }
        }
        return insecureSslFactory;
    }

    /**
     * 单节点 WebSocket/TLS 探测
     * 发送真实的 WebSocket Upgrade 握手请求，并精准计算 RTT 往返时延（毫秒）
     * @return 延迟 ms (>=1)；若不可用/超时返回 LATENCY_TIMEOUT (-1)
     */
    public static int testProfile(String wssAddr, String token, String prefIp, boolean insecure) {
        if (TextUtils.isEmpty(wssAddr)) {
            return LATENCY_TIMEOUT;
        }

        Socket plainSocket = null;
        Socket socket = null;
        try {
            String urlStr = wssAddr.trim();
            if (!urlStr.contains("://")) {
                urlStr = "wss://" + urlStr;
            }

            boolean isTls = !urlStr.startsWith("ws://") && !urlStr.startsWith("http://");
            String httpUrl = urlStr.replaceFirst("^(?i)wss://", "https://").replaceFirst("^(?i)ws://", "http://");
            URL parsed = new URL(httpUrl);
            String host = parsed.getHost();
            int port = parsed.getPort();
            if (port <= 0) {
                port = isTls ? 443 : 80;
            }
            String path = parsed.getPath();
            if (TextUtils.isEmpty(path)) {
                path = "/";
            }

            String targetConnectHost = host;
            if (!TextUtils.isEmpty(prefIp)) {
                String[] parts = prefIp.split(",");
                if (parts.length > 0 && !parts[0].trim().isEmpty()) {
                    targetConnectHost = parts[0].trim();
                }
            }

            long start = System.currentTimeMillis();

            // 1. TCP 握手
            plainSocket = new Socket();
            plainSocket.connect(new InetSocketAddress(targetConnectHost, port), TIMEOUT_MS);
            plainSocket.setSoTimeout(TIMEOUT_MS);

            // 2. TLS 握手 (若为 WSS/TLS)
            if (isTls) {
                SSLSocketFactory sf = insecure ? getInsecureSslFactory() : (SSLSocketFactory) SSLSocketFactory.getDefault();
                SSLSocket sslSocket = (SSLSocket) sf.createSocket(plainSocket, host, port, true);
                try {
                    SSLParameters params = sslSocket.getSSLParameters();
                    params.setServerNames(Collections.singletonList(new javax.net.ssl.SNIHostName(host)));
                    sslSocket.setSSLParameters(params);
                } catch (Throwable ignored) {}
                sslSocket.startHandshake();
                socket = sslSocket;
            } else {
                socket = plainSocket;
            }

            // 3. 发送 WebSocket Upgrade 请求
            OutputStream out = socket.getOutputStream();
            StringBuilder req = new StringBuilder();
            req.append("GET ").append(path).append(" HTTP/1.1\r\n");
            req.append("Host: ").append(host).append("\r\n");
            req.append("Upgrade: websocket\r\n");
            req.append("Connection: Upgrade\r\n");
            req.append("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n");
            req.append("Sec-WebSocket-Version: 13\r\n");
            if (!TextUtils.isEmpty(token)) {
                req.append("Sec-WebSocket-Protocol: ").append(token.trim()).append("\r\n");
            }
            req.append("\r\n");

            out.write(req.toString().getBytes("UTF-8"));
            out.flush();

            // 4. 读取 HTTP 响应首行
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
            String statusLine = reader.readLine();

            long elapsed = System.currentTimeMillis() - start;

            if (statusLine != null && statusLine.startsWith("HTTP/")) {
                return Math.max(1, (int) elapsed);
            }
            return LATENCY_TIMEOUT;
        } catch (Throwable t) {
            Log.d(TAG, "Test failed for " + wssAddr + ": " + t.getMessage());
            return LATENCY_TIMEOUT;
        } finally {
            if (socket != null) {
                try { socket.close(); } catch (Throwable ignored) {}
            }
            if (plainSocket != null && !plainSocket.isClosed()) {
                try { plainSocket.close(); } catch (Throwable ignored) {}
            }
        }
    }

    /**
     * 针对当前已连接 VPN 隧道测试出网真实连通性
     * @return 实际往返时延 ms；若失败返回 LATENCY_TIMEOUT (-1)
     */
    public static int testCurrentVpnConnectivity() {
        String[] testUrls = new String[]{
                "http://cp.cloudflare.com/generate_204",
                "http://www.google.com/generate_204"
        };

        for (String testUrl : testUrls) {
            HttpURLConnection conn = null;
            try {
                long start = System.currentTimeMillis();
                URL url = new URL(testUrl);
                conn = (HttpURLConnection) url.openConnection();
                conn.setConnectTimeout(3000);
                conn.setReadTimeout(3000);
                conn.setUseCaches(false);
                conn.setRequestMethod("GET");
                conn.connect();
                int code = conn.getResponseCode();
                long elapsed = System.currentTimeMillis() - start;
                if (code == 204 || code == 200) {
                    return Math.max(1, (int) elapsed);
                }
            } catch (Throwable ignored) {
            } finally {
                if (conn != null) {
                    try { conn.disconnect(); } catch (Throwable ignored) {}
                }
            }
        }
        return LATENCY_TIMEOUT;
    }

    /**
     * 并发测试所有节点与当前网络
     */
    public static void testAllProfiles(Preferences prefs, List<String> profileIds, boolean testCurrentTunnel, TestCallback callback) {
        if (profileIds == null || profileIds.isEmpty()) {
            if (callback != null) {
                mainHandler.post(() -> callback.onComplete(0, 0, 0, LATENCY_TIMEOUT));
            }
            return;
        }

        final int total = profileIds.size();
        final AtomicInteger completed = new AtomicInteger(0);
        final AtomicInteger available = new AtomicInteger(0);
        final AtomicInteger failed = new AtomicInteger(0);

        for (String id : profileIds) {
            // 先通知 UI 标记该节点测速中
            if (callback != null) {
                callback.onProgress(id, LATENCY_TESTING);
            }

            executor.execute(() -> {
                String wssAddr = prefs.getWssAddr(id);
                String token = prefs.getToken(id);
                String prefIp = prefs.getPrefIp(id);
                boolean insecure = prefs.getInsecure(id);

                int latency = testProfile(wssAddr, token, prefIp, insecure);
                if (latency > 0) {
                    available.incrementAndGet();
                } else {
                    failed.incrementAndGet();
                }

                if (callback != null) {
                    mainHandler.post(() -> callback.onProgress(id, latency));
                }

                if (completed.incrementAndGet() == total) {
                    // 全部节点测试完成，如果需要，测当前连接连通性
                    int currentLatency = testCurrentTunnel ? testCurrentVpnConnectivity() : LATENCY_TIMEOUT;
                    if (callback != null) {
                        mainHandler.post(() -> callback.onComplete(total, available.get(), failed.get(), currentLatency));
                    }
                }
            });
        }
    }
}
