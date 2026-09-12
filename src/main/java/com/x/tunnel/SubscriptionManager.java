package com.x.tunnel;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import android.util.Base64;
import android.util.Log;

public class SubscriptionManager {
    private static final String TAG = "SubscriptionManager";

    public static class SubNode {
        public String name;
        public String server; // WssAddr
        public String token;  // Token
        public String ip;     // PrefIp
        public boolean fallback = true; // fallback=1 对应 disableEch (停用ECH走标准TLS)
        public boolean insecure = false; // insecure=1 对应跳过证书验证
        public int connections = 3;
        public String block = "443";
    }

    public interface Callback {
        void onSuccess(int count);
        void onError(String message);
    }

    public static void fetchAndUpdate(Preferences prefs, String subUrl, Callback callback) {
        new Thread(() -> {
            try {
                if (subUrl == null || subUrl.trim().isEmpty()) {
                    if (callback != null) callback.onError("订阅地址为空");
                    return;
                }

                String content = downloadUrl(subUrl.trim());
                if (content == null || content.trim().isEmpty()) {
                    if (callback != null) callback.onError("获取订阅内容为空");
                    return;
                }

                // 尝试检测是否为纯 Base64 编码，是则先解码
                content = tryBase64Decode(content.trim());

                List<SubNode> nodes = parseIniNodes(content);
                if (nodes.isEmpty()) {
                    if (callback != null) callback.onError("未解析到有效节点配置");
                    return;
                }

                applySubNodes(prefs, nodes);
                prefs.setSubLastSyncTime(System.currentTimeMillis());

                if (callback != null) {
                    callback.onSuccess(nodes.size());
                }
            } catch (Exception e) {
                Log.e(TAG, "Sync subscription failed", e);
                if (callback != null) {
                    callback.onError("同步失败: " + e.getMessage());
                }
            }
        }).start();
    }

    private static String downloadUrl(String urlStr) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(15000);
        conn.setRequestProperty("User-Agent", "X-Tunnel-Sub/1.0");

        int respCode = conn.getResponseCode();
        if (respCode >= 300 && respCode < 400) {
            String redirectUrl = conn.getHeaderField("Location");
            if (redirectUrl != null) {
                return downloadUrl(redirectUrl);
            }
        }

        if (respCode != 200) {
            throw new Exception("HTTP " + respCode);
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            return sb.toString();
        } finally {
            conn.disconnect();
        }
    }

    private static String tryBase64Decode(String text) {
        if (text.startsWith("[") || text.contains("[Server") || text.contains("server=")) {
            return text;
        }
        try {
            byte[] decoded = Base64.decode(text, Base64.DEFAULT);
            String str = new String(decoded, StandardCharsets.UTF_8);
            if (str.contains("[") || str.contains("server=")) {
                return str;
            }
        } catch (Throwable ignored) {}
        return text;
    }

    public static List<SubNode> parseIniNodes(String content) {
        List<SubNode> list = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new StringReader(content))) {
            String line;
            String currentSection = null;
            Map<String, Map<String, String>> sections = new LinkedHashMap<>();

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#") || line.startsWith(";")) {
                    continue;
                }
                if (line.startsWith("[") && line.endsWith("]")) {
                    currentSection = line.substring(1, line.length() - 1).trim();
                    if (!sections.containsKey(currentSection)) {
                        sections.put(currentSection, new LinkedHashMap<>());
                    }
                    continue;
                }

                if (currentSection != null) {
                    int eq = line.indexOf('=');
                    if (eq > 0) {
                        String key = line.substring(0, eq).trim().toLowerCase();
                        String val = line.substring(eq + 1).trim();
                        sections.get(currentSection).put(key, val);
                    }
                }
            }

            // 过滤提取 ServerX 节
            int index = 1;
            for (Map.Entry<String, Map<String, String>> entry : sections.entrySet()) {
                String secName = entry.getKey();
                if (secName.equalsIgnoreCase("Settings")) {
                    continue;
                }
                Map<String, String> kv = entry.getValue();
                String server = kv.get("server");
                if (server != null && !server.isEmpty()) {
                    SubNode node = new SubNode();
                    node.server = server;
                    node.token = kv.getOrDefault("token", "");
                    node.ip = kv.getOrDefault("ip", "");

                    String fbStr = kv.get("fallback");
                    if (fbStr != null) {
                        node.fallback = "1".equals(fbStr.trim()) || "true".equalsIgnoreCase(fbStr.trim());
                    }
                    String insStr = kv.get("insecure");
                    if (insStr != null) {
                        node.insecure = "1".equals(insStr.trim()) || "true".equalsIgnoreCase(insStr.trim());
                    }
                    String connStr = kv.get("connections");
                    if (connStr != null) {
                        try {
                            node.connections = Integer.parseInt(connStr.trim());
                        } catch (Exception ignored) {}
                    }
                    String blockStr = kv.get("block");
                    if (blockStr != null && !blockStr.trim().isEmpty()) {
                        node.block = blockStr.trim();
                    }

                    String name = kv.get("name");
                    if (name == null || name.isEmpty()) {
                        name = "节点 " + index;
                    }
                    node.name = name;
                    list.add(node);
                    index++;
                }
            }
        } catch (Exception e) {
            Log.e(TAG, "parse INI error", e);
        }
        return list;
    }

    private static synchronized void applySubNodes(Preferences prefs, List<SubNode> newNodes) {
        // 1. 获取所有旧的 profile
        Set<String> oldIds = prefs.getProfileIds();
        String currentId = prefs.getCurrentProfileId();
        String currentServerBefore = prefs.getWssAddr(currentId);

        // 2. 清理旧的订阅节点（保留用户手动创建的节点）
        for (String id : oldIds) {
            if (prefs.isSubProfile(id)) {
                prefs.removeProfile(id);
            }
        }

        // 3. 添加新的订阅节点
        String firstAddedId = null;
        String matchedCurrentId = null;

        for (SubNode node : newNodes) {
            String newId = "sub_" + UUID.randomUUID().toString().substring(0, 8);
            if (firstAddedId == null) {
                firstAddedId = newId;
            }
            prefs.addProfile(newId, node.name);
            prefs.setProfileIsSub(newId, true);

            prefs.setWssAddr(newId, node.server);
            prefs.setToken(newId, node.token);
            prefs.setPrefIp(newId, node.ip);

            // 属性根据订阅配置自动注入
            prefs.setWsConn(newId, prefs.clampWsConn(node.connections));
            prefs.setUdpBlockPorts(newId, node.block);
            prefs.setEchDns(newId, "https://doh.pub/dns-query");
            prefs.setEchDomain(newId, "cloudflare-ech.com");
            // fallback=1 对应停用 ECH (走标准 TLS)，避免 Cloudflare Tunnel 节点因 ECH 握手失败
            prefs.setDisableEch(newId, node.fallback);
            prefs.setInsecure(newId, node.insecure);

            if (currentServerBefore != null && !currentServerBefore.isEmpty() && currentServerBefore.equals(node.server)) {
                matchedCurrentId = newId;
            }
        }

        // 4. 处理当前选中的节点ID
        Set<String> currentIds = prefs.getProfileIds();
        boolean currentValid = currentIds.contains(currentId) && !prefs.getWssAddr(currentId).trim().isEmpty();
        if (!currentValid) {
            if (matchedCurrentId != null) {
                prefs.setCurrentProfileId(matchedCurrentId);
            } else if (firstAddedId != null) {
                prefs.setCurrentProfileId(firstAddedId);
            } else if (!currentIds.isEmpty()) {
                prefs.setCurrentProfileId(currentIds.iterator().next());
            }
        }
    }
}
