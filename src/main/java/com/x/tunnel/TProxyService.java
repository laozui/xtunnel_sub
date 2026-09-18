/*
 ============================================================================
 Name        : TProxyService.java
 Author      : hev <r@hev.cc>
 Copyright   : Copyright (c) 2024 xyz
 Description : TProxy Service
 ============================================================================
 */

package com.x.tunnel;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.content.Context;
import android.os.Build;
import android.os.ParcelFileDescriptor;
import android.app.Notification;
import android.app.Notification.Builder;
import android.app.PendingIntent;
import android.content.Intent;
import android.net.VpnService;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;
import android.widget.Toast;
import com.x.tunnel.tunnel.Tunnel;
import android.content.pm.PackageManager.NameNotFoundException;
import android.content.pm.ServiceInfo;

import androidx.core.app.NotificationCompat;

public class TProxyService extends VpnService {
	private static native void TProxyStartService(String config_path, int fd);
	private static native void TProxyStopService();
	private static native long[] TProxyGetStats();

        public static final String ACTION_CONNECT = "com.x.tunnel.CONNECT";
        public static final String ACTION_DISCONNECT = "com.x.tunnel.DISCONNECT";
        public static final String ACTION_SWITCH = "com.x.tunnel.SWITCH";

        public static final String EXTRA_PROFILE_ID = "profile_id";
        public static final String EXTRA_PROFILE_NAME = "profile_name";
        public static final String EXTRA_WSS_ADDR = "wss_addr";
        public static final String EXTRA_WS_CONN = "ws_conn";
        public static final String EXTRA_UDP_BLOCK_PORTS = "udp_block_ports";
        public static final String EXTRA_ECH_DNS = "ech_dns";
        public static final String EXTRA_ECH_DOMAIN = "ech_domain";
        public static final String EXTRA_PREF_IP = "pref_ip";
        public static final String EXTRA_TOKEN = "token";
        public static final String EXTRA_DISABLE_ECH = "disable_ech";
        public static final String EXTRA_IPS_PREF = "ips_pref";
        public static final String EXTRA_INSECURE = "insecure";

	static {
		System.loadLibrary("hev-socks5-tunnel");
	}

	private ParcelFileDescriptor tunFd = null;
        private final Object switchLock = new Object();
        private volatile int currentSwitchSeq = 0;

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent != null && ACTION_DISCONNECT.equals(intent.getAction())) {
            stopService();
            return START_NOT_STICKY;
        }
        if (intent != null && ACTION_SWITCH.equals(intent.getAction())) {
            if (tunFd != null) {
                switchProfile(intent);
                return START_STICKY;
            }
            startService();
            return START_STICKY;
        }
        if (tunFd != null) {
            return START_STICKY;
        }
        startService();
        return START_STICKY;
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
    }

	@Override
	public void onRevoke() {
		stopService();
		super.onRevoke();
	}

    public void startService() {
        if (tunFd != null) { return; }

		Preferences prefs = new Preferences(this);

		/* VPN */
		String session = new String();
        VpnService.Builder builder = new VpnService.Builder();
		builder.setBlocking(false);
		builder.setMtu(prefs.getTunnelMtu());
		String addr4 = prefs.getTunnelIpv4Address();
		int prefix4 = prefs.getTunnelIpv4Prefix();
		String dns4 = prefs.getDnsIpv4();
		builder.addAddress(addr4, prefix4);
		builder.addRoute("0.0.0.0", 0);
		if (!prefs.getRemoteDns() && !dns4.isEmpty())
		  builder.addDnsServer(dns4);
		session += "IPv4";

		String addr6 = prefs.getTunnelIpv6Address();
		int prefix6 = prefs.getTunnelIpv6Prefix();
		String dns6 = prefs.getDnsIpv6();
		builder.addAddress(addr6, prefix6);
		builder.addRoute("::", 0);
		if (!prefs.getRemoteDns() && !dns6.isEmpty())
		  builder.addDnsServer(dns6);
		session += " + IPv6";
		if (prefs.getRemoteDns()) {
			builder.addDnsServer(prefs.getMappedDns());
		}
		boolean disallowSelf = true;
		if (prefs.getGlobal()) {
			session += "/Global";
		} else {
			for (String appName : prefs.getApps()) {
				try {
					builder.addAllowedApplication(appName);
					disallowSelf = false;
				} catch (NameNotFoundException e) {
				}
			}
			session += "/per-App";
		}
		if (disallowSelf) {
			String selfName = getApplicationContext().getPackageName();
			try {
				builder.addDisallowedApplication(selfName);
			} catch (NameNotFoundException e) {
			}
		}
		builder.setSession(session);
        try {
            tunFd = builder.establish();
        } catch (Exception e) {
            stopSelf();
            return;
        }
        if (tunFd == null) {
            stopSelf();
            return;
        }

                File tproxy_file = new File(getCacheDir(), "tproxy.conf");
                try {
                        tproxy_file.createNewFile();
                        FileOutputStream fos = new FileOutputStream(tproxy_file, false);

                        String tproxy_conf = "misc:\n" +
                                "  task-stack-size: " + prefs.getTaskStackSize() + "\n" +
                                "tunnel:\n" +
                                "  mtu: " + prefs.getTunnelMtu() + "\n";

                        tproxy_conf += "socks5:\n" +
                                "  port: " + prefs.getSocksPort() + "\n" +
                                "  address: '127.0.0.1'\n" +
                                "  udp: '" + (prefs.getUdpInTcp() ? "tcp" : "udp") + "'\n";

                        if (!prefs.getSocksUdpAddress().isEmpty()) {
                                tproxy_conf += "  udp-address: '" + prefs.getSocksUdpAddress() + "'\n";
                        }

                        if (!prefs.getSocksUsername().isEmpty() &&
                                !prefs.getSocksPassword().isEmpty()) {
                                tproxy_conf += "  username: '" + prefs.getSocksUsername() + "'\n";
                                tproxy_conf += "  password: '" + prefs.getSocksPassword() + "'\n";
                        }

                        if (prefs.getRemoteDns()) {
                                tproxy_conf += "mapdns:\n" +
                                        "  address: " + prefs.getMappedDns() + "\n" +
                                        "  port: 53\n" +
                                        "  network: 240.0.0.0\n" +
                                        "  netmask: 240.0.0.0\n" +
                                        "  cache-size: 10000\n";
                        }

                        fos.write(tproxy_conf.getBytes());
                        fos.close();
                } catch (IOException e) {
                        return;
                }
                
                TProxyStartService(tproxy_file.getAbsolutePath(), tunFd.getFd());

                try {
                        String wsAddr = prefs.getWssAddr().trim();
                        String wsAddrLower = wsAddr.toLowerCase();
                        if (!wsAddrLower.startsWith("wss://") && !wsAddrLower.startsWith("ws://")) {
                                wsAddr = "wss://" + wsAddr;
                        }
                        int idx = wsAddr.indexOf("://");
                        if (idx >= 0) {
                                String rest = wsAddr.substring(idx + 3);
                                if (!rest.contains("/")) {
                                        wsAddr = wsAddr + "/";
                                }
                        }
                        Tunnel.startSocksProxy(
                                prefs.getSocksAddress() + ":" + Integer.toString(prefs.getSocksPort()),
                                wsAddr,
                                prefs.getWsConn(),
                                prefs.getUdpBlockPorts(),
                                prefs.getEchDns(),
                                prefs.getEchDomain(),
                                prefs.getPrefIp(),
                                prefs.getToken(),
                                prefs.getDisableEch(),
                                prefs.getIpsPref(),
                                prefs.getInsecure()
                        );
                        final Context appContext = getApplicationContext();
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                boolean ok = false;
                                try {
                                    ok = Tunnel.waitSocksProxyReady(3000);
                                } catch (Throwable t) {
                                }
                                if (ok) {
                                    new Handler(Looper.getMainLooper()).post(new Runnable() {
                                        @Override
                                        public void run() {
                                            Toast.makeText(appContext, "启动成功", Toast.LENGTH_SHORT).show();
                                        }
                                    });
                                    Log.i("xtunnel", "===== 启动成功 =====");
                                } else {
                                    new Handler(Looper.getMainLooper()).post(new Runnable() {
                                        @Override
                                        public void run() {
                                            Toast.makeText(appContext, "服务器连接失败", Toast.LENGTH_SHORT).show();
                                        }
                                    });
                                    Log.e("xtunnel", "===== 服务器连接失败 =====");
                                }
                            }
                        }, "xtunnel-ws-wait").start();
                } catch (Exception e) {
                        try { TProxyStopService(); } catch (Throwable t) {}
                        try { if (tunFd != null) { tunFd.close(); tunFd = null; } } catch (IOException ioe) {}
                        stopSelf();
                        return;
                }
                prefs.setEnable(true);

		String channelName = "socks5";
		initNotificationChannel(channelName);
		createNotification(channelName);
	}

    /**
     * 运行中无缝热切换节点（Hot Switch）
     * - 保持虚拟 TUN 网卡 (tunFd) 与底层 hev-socks5-tunnel 运转不中断
     * - 仅在后台原子重启 Go 层的 SOCKS5 代理内核，并连接到目标节点
     * - 具备排队防重入与序列号保护，避免用户连续点击导致竞态冲突
     */
    public void switchProfile(Intent intent) {
        final int seq = ++currentSwitchSeq;
        final String profileId = intent.getStringExtra(EXTRA_PROFILE_ID);
        final String profileName = intent.getStringExtra(EXTRA_PROFILE_NAME);

        Preferences localPrefs = new Preferences(this);
        final String rawWsAddr = intent.hasExtra(EXTRA_WSS_ADDR)
                ? intent.getStringExtra(EXTRA_WSS_ADDR)
                : localPrefs.getWssAddr(profileId);
        final int wsConn = intent.hasExtra(EXTRA_WS_CONN)
                ? intent.getIntExtra(EXTRA_WS_CONN, 3)
                : localPrefs.getWsConn(profileId);
        final String udpBlockPorts = intent.hasExtra(EXTRA_UDP_BLOCK_PORTS)
                ? intent.getStringExtra(EXTRA_UDP_BLOCK_PORTS)
                : localPrefs.getUdpBlockPorts(profileId);
        final String echDns = intent.hasExtra(EXTRA_ECH_DNS)
                ? intent.getStringExtra(EXTRA_ECH_DNS)
                : localPrefs.getEchDns(profileId);
        final String echDomain = intent.hasExtra(EXTRA_ECH_DOMAIN)
                ? intent.getStringExtra(EXTRA_ECH_DOMAIN)
                : localPrefs.getEchDomain(profileId);
        final String prefIp = intent.hasExtra(EXTRA_PREF_IP)
                ? intent.getStringExtra(EXTRA_PREF_IP)
                : localPrefs.getPrefIp(profileId);
        final String token = intent.hasExtra(EXTRA_TOKEN)
                ? intent.getStringExtra(EXTRA_TOKEN)
                : localPrefs.getToken(profileId);
        final boolean disableEch = intent.hasExtra(EXTRA_DISABLE_ECH)
                ? intent.getBooleanExtra(EXTRA_DISABLE_ECH, false)
                : localPrefs.getDisableEch(profileId);
        final String ipsPref = intent.hasExtra(EXTRA_IPS_PREF)
                ? intent.getStringExtra(EXTRA_IPS_PREF)
                : localPrefs.getIpsPref(profileId);
        final boolean insecure = intent.hasExtra(EXTRA_INSECURE)
                ? intent.getBooleanExtra(EXTRA_INSECURE, false)
                : localPrefs.getInsecure(profileId);

        final String socksAddr = localPrefs.getSocksAddress() + ":" + localPrefs.getSocksPort();
        final Context appContext = getApplicationContext();

        new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (switchLock) {
                    if (seq != currentSwitchSeq) {
                        return;
                    }
                    String displayName = (profileName != null && !profileName.isEmpty()) ? profileName : profileId;
                    Log.i("xtunnel", "===== 开始热切换节点: " + displayName + " =====");

                    // 1. 优雅停止旧代理内核（释放原有 websocket 连接池）
                    try {
                        Tunnel.stopSocksProxy();
                    } catch (Throwable t) {
                        Log.w("xtunnel", "停止旧代理内核提示: " + t.getMessage());
                    }

                    // 2. 缓冲 100ms 确保端口与连接释放完全
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {}

                    if (seq != currentSwitchSeq) {
                        return;
                    }

                    // 3. 规范化 ws 地址
                    String wsAddr = rawWsAddr != null ? rawWsAddr.trim() : "";
                    String wsAddrLower = wsAddr.toLowerCase();
                    if (!wsAddrLower.startsWith("wss://") && !wsAddrLower.startsWith("ws://")) {
                        wsAddr = "wss://" + wsAddr;
                    }
                    int idx = wsAddr.indexOf("://");
                    if (idx >= 0) {
                        String rest = wsAddr.substring(idx + 3);
                        if (!rest.contains("/")) {
                            wsAddr = wsAddr + "/";
                        }
                    }

                    // 4. 拉起新节点内核
                    try {
                        Tunnel.startSocksProxy(
                                socksAddr,
                                wsAddr,
                                wsConn,
                                udpBlockPorts,
                                echDns,
                                echDomain,
                                prefIp,
                                token,
                                disableEch,
                                ipsPref,
                                insecure
                        );
                    } catch (Throwable e) {
                        Log.e("xtunnel", "热切换启动新内核失败: " + e.getMessage(), e);
                        new Handler(Looper.getMainLooper()).post(new Runnable() {
                            @Override
                            public void run() {
                                Toast.makeText(appContext, "节点切换失败: " + e.getMessage(), Toast.LENGTH_SHORT).show();
                            }
                        });
                        return;
                    }

                    // 5. 等待新节点握手就绪
                    boolean ok = false;
                    try {
                        ok = Tunnel.waitSocksProxyReady(3000);
                    } catch (Throwable ignored) {}

                    final boolean success = ok;
                    new Handler(Looper.getMainLooper()).post(new Runnable() {
                        @Override
                        public void run() {
                            if (success) {
                                Toast.makeText(appContext, "已无缝切换至: " + displayName, Toast.LENGTH_SHORT).show();
                                Log.i("xtunnel", "===== 热切换节点成功: " + displayName + " =====");
                            } else {
                                Toast.makeText(appContext, "节点已切换，正在连接中…", Toast.LENGTH_SHORT).show();
                                Log.w("xtunnel", "===== 节点握手未在3s内完成，后台继续连接 =====");
                            }
                        }
                    });

                    // 6. 刷新通知栏常驻显示
                    updateNotification(displayName);
                }
            }
        }, "xtunnel-switch-" + seq).start();
    }

    public void stopService() {
        try { stopForeground(true); } catch (Throwable t) { }
        try { Tunnel.stopSocksProxy(); } catch (Exception e) { }
        try { TProxyStopService(); } catch (Throwable t) { }
        if (tunFd != null) {
            try { tunFd.close(); } catch (IOException e) {}
            tunFd = null;
        }
        System.exit(0);
    }

    private void startDiagnosticsLogger() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    try { Thread.sleep(5000); } catch (InterruptedException e) { break; }
                    try {
                        // 直接读取 Go 侧写入的诊断文件(不依赖 gomobile 方法绑定)
                        java.io.File f = new java.io.File(getFilesDir(), "xtunnel_status.log");
                        if (f.exists()) {
                            java.io.BufferedReader r = new java.io.BufferedReader(new java.io.FileReader(f));
                            StringBuilder sb = new StringBuilder();
                            String line;
                            while ((line = r.readLine()) != null) { sb.append(line).append("\n"); }
                            r.close();
                            Log.i("xtunnel", "===== TUNNEL STATUS =====\n" + sb.toString());
                        }
                    } catch (Throwable t) {
                        Log.e("xtunnel", "diag error", t);
                    }
                }
            }
        }, "xtunnel-diag").start();
    }

    private void writeStatusFile(String status) {
        try {
            java.io.File f = new java.io.File(getFilesDir(), "xtunnel_status.log");
            java.io.FileWriter w = new java.io.FileWriter(f, false);
            w.write(status);
            w.close();
        } catch (Throwable t) { }
    }

    private void runWithTimeout(Runnable task, long timeoutMs, String name) {
        Thread th = new Thread(task, name);
        th.start();
        try { th.join(timeoutMs); } catch (InterruptedException ignored) {}
    }

	private void createNotification(String channelName) {
		Intent i = new Intent(this, MainActivity.class);
		i.setFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP | Intent.FLAG_ACTIVITY_SINGLE_TOP);
		PendingIntent pi = PendingIntent.getActivity(this, 0, i, PendingIntent.FLAG_IMMUTABLE);
		NotificationCompat.Builder notification = new NotificationCompat.Builder(this, channelName);
        Preferences prefs = new Preferences(this);
        String profileName = prefs.getProfileName(prefs.getCurrentProfileId());
        String content = (profileName != null && !profileName.isEmpty())
                ? "当前节点: " + profileName
                : getString(R.string.status_connected);
		Notification notify = notification
				.setContentTitle(getString(R.string.app_name))
                .setContentText(content)
				.setSmallIcon(android.R.drawable.sym_def_app_icon)
				.setContentIntent(pi)
				.build();
		if (Build.VERSION.SDK_INT < Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
			startForeground(1, notify);
		} else {
			startForeground(1, notify, ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE);
		}
	}

    private void updateNotification(String profileName) {
        String channelName = "socks5";
        initNotificationChannel(channelName);
        Intent i = new Intent(this, MainActivity.class);
        i.setFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP | Intent.FLAG_ACTIVITY_SINGLE_TOP);
        PendingIntent pi = PendingIntent.getActivity(this, 0, i, PendingIntent.FLAG_IMMUTABLE);
        NotificationCompat.Builder notification = new NotificationCompat.Builder(this, channelName);
        String content = (profileName != null && !profileName.isEmpty())
                ? "当前节点: " + profileName
                : getString(R.string.status_connected);
        Notification notify = notification
                .setContentTitle(getString(R.string.app_name))
                .setContentText(content)
                .setSmallIcon(android.R.drawable.sym_def_app_icon)
                .setContentIntent(pi)
                .build();
        NotificationManager nm = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
        if (nm != null) {
            nm.notify(1, notify);
        }
    }

	private void initNotificationChannel(String channelName) {
		NotificationManager notificationManager = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
		if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
			CharSequence name = getString(R.string.app_name);
			NotificationChannel channel = new NotificationChannel(channelName, name, NotificationManager.IMPORTANCE_DEFAULT);
			notificationManager.createNotificationChannel(channel);
		}
	}
}
