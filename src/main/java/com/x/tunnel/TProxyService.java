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

	/** 供看门狗读取原生层收发字节数（判断是否还在真正转发）。 */
	static long[] nativeStats() {
		return TProxyGetStats();
	}

        public static final String ACTION_CONNECT = "com.x.tunnel.CONNECT";
        public static final String ACTION_DISCONNECT = "com.x.tunnel.DISCONNECT";
        public static final String ACTION_SWITCH = "com.x.tunnel.SWITCH";
        /* 深度重连：关 TUN + 停原生层 + 停内核 + 重建，等价于关掉 App 重开，但不杀进程。
           由 TunnelWatchdog 在判定隧道僵死时触发。 */
        public static final String ACTION_HEAL = "com.x.tunnel.HEAL";

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
        /* 深度重连互斥闸：同一时刻只允许一次拆建在跑。 */
        private final java.util.concurrent.atomic.AtomicBoolean healInProgress =
                new java.util.concurrent.atomic.AtomicBoolean(false);

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent != null && ACTION_DISCONNECT.equals(intent.getAction())) {
            stopService();
            return START_NOT_STICKY;
        }
        if (intent != null && ACTION_HEAL.equals(intent.getAction())) {
            healTunnel(intent);
            return START_STICKY;
        }
        if (intent != null && ACTION_SWITCH.equals(intent.getAction())) {
            if (tunFd != null) {
                switchProfile(intent);
                return START_STICKY;
            }
            /* 深度重连期间 tunFd 被主动置空了。这时如果用户点「切换线路」，
               老逻辑会直接 startService() 去建 TUN —— 和正在重建的线程抢，
               结果是一个刚 establish 就被另一个 close 掉。
               重建本来就会从 prefs 读最新节点，所以这里直接忽略即可。 */
            if (healInProgress.get()) {
                Log.i("xtunnel", "深度重连进行中，忽略切换请求（重建会自动用最新节点）");
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

		/* ★ 启动自愈看门狗：跑起来之后没人盯着，隧道僵死了界面也显示"已连接"。
		   看门狗每 30 秒做三路取证，连续 3 轮僵死就触发深度重连。 */
		TunnelWatchdog.start(this);

		/* 把闲置已久的诊断日志真正跑起来（幂等）：万一还复发，
		   logcat 里至少有 Go 侧状态快照 + 看门狗判定过程两份现场。 */
		startDiagnosticsLogger();
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

                    /* 7. 热切换成功后重置看门狗基线：
                          新连接池的字节计数与旧池不连续，不重置会被误判成"僵死"。 */
                    TunnelWatchdog.notifySwitched();
                }
            }
        }, "xtunnel-switch-" + seq).start();
    }

    /**
     * 深度重连（自愈）：把整条隧道彻底拆掉再重建。
     *
     * 为什么不能复用 {@link #switchProfile}：
     * 那个只重启 Go 侧的 SOCKS5 + WS 连接池，**TUN 网卡和原生转发层完全不动**。
     * 老豆的实测恰恰是「切线路也没用」—— 说明僵死发生在这两层，
     * 只重启内核等于隔靴搔痒。
     *
     * 与 stopService() 的区别：那边直接 System.exit(0) 杀进程，
     * 用户会看到通知栏消失、需要重新授权；这里在**同一进程内**优雅重建 ——
     * 原生层 native_stop_service 已确认会 quit + pthread_join 并复位 is_working，
     * 所以可以安全地停了再起。效果等同于关机重开，但用户几乎无感。
     *
     * 注意：真实的拆除/重建放在后台线程。onStartCommand 跑在主线程，
     * 里面有 Thread.sleep(500) 等 fd 释放 —— 放主线程会卡界面甚至 ANR。
     */
    public void healTunnel(Intent intent) {
        /* 互斥：万一短时间内来了两个 HEAL（比如看门狗恰好和别处触发撞上），
           绝不能让两个线程同时拆建 —— 那会把刚建好的 TUN 又拆掉。
           已经在重连就直接忽略，反正目标状态一样。 */
        if (!healInProgress.compareAndSet(false, true)) {
            Log.i("xtunnel", "已有一次深度重连在进行中，忽略本次请求");
            return;
        }
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    doHealInBackground();
                } finally {
                    healInProgress.set(false);
                }
            }
        }, "xtunnel-heal").start();
    }

    private void doHealInBackground() {
        Log.w("xtunnel", "===== 开始深度重连（自愈） =====");

        Preferences prefs = new Preferences(this);
        if (!prefs.getEnable()) {
            Log.i("xtunnel", "深度重连取消：用户已关闭 VPN");
            return;
        }

        /* 立刻让看门狗闭嘴：下面要拆 TUN，拆完到重建完成之间探测必然不通，
           不闭麦就会出现"拆→探不通→再判僵死→再拆"的死循环。
           必须放在 try/finally 里 —— 万一重建抛异常直接 return，
           rebuilding 会永远停在 true，看门狗就再也不干活了。 */
        TunnelWatchdog.beginRebuild();
        try {
            /* 1. 彻底拆掉旧隧道。顺序很重要：
                  先关 TUN（切断 App 流量入口）→ 再停原生转发层 → 最后停 Go 内核。
                  反过来做会让原生层在 TUN 已消失的情况下继续收包，产生大量无主错误。 */
            try {
                if (tunFd != null) {
                    try { tunFd.close(); } catch (Throwable ignored) { }
                    tunFd = null;
                    Log.i("xtunnel", "深度重连[1/5] 已关闭 TUN 网卡");
                }
            } catch (Throwable t) {
                Log.w("xtunnel", "关闭 TUN 异常: " + t.getMessage());
            }

            try {
                TProxyStopService();
                Log.i("xtunnel", "深度重连[2/5] 已停止原生转发层");
            } catch (Throwable t) {
                Log.w("xtunnel", "停止原生转发层异常: " + t.getMessage());
            }

            try {
                Tunnel.stopSocksProxy();
                Log.i("xtunnel", "深度重连[3/5] 已停止 Go 内核");
            } catch (Throwable t) {
                Log.w("xtunnel", "停止 Go 内核异常: " + t.getMessage());
            }

            /* 2. 给系统一点时间真正回收 fd / 端口 / 原生线程。
                  原生层 pthread_join 是同步的，但 socket fd 的最后引用可能还在
                  内核的 TIME_WAIT 里；500ms 是实测下来既稳又不拖沓的值。 */
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
            }

            /* 3. 重建。startService() 内部会重新 establish TUN、重启原生层、重连内核，
                  并用 prefs 里最新的节点/优选 IP 配置 —— 不需要在这里再拼参数。
                  ★ 这一步必须回主线程做：平时 startService() 都是从 onStartCommand
                  在主线程调起来的，VpnService.establish() 与界面/通知栏的交互按主线程
                  语义设计；塞在后台线程跑虽然大多能成，但没理由去赌。这里投递到主线程
                  并等它跑完（最多 20 秒），保证 endRebuild() 是在隧道真的重建之后才执行。 */
            final java.util.concurrent.CountDownLatch done =
                    new java.util.concurrent.CountDownLatch(1);
            final Throwable[] err = new Throwable[1];
            new Handler(Looper.getMainLooper()).post(new Runnable() {
                @Override
                public void run() {
                    try {
                        startService();
                    } catch (Throwable t) {
                        err[0] = t;
                    } finally {
                        done.countDown();
                    }
                }
            });
            try {
                if (!done.await(20, java.util.concurrent.TimeUnit.SECONDS)) {
                    Log.w("xtunnel", "深度重连：重建步骤等待超时（主线程繁忙），仍继续");
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            if (err[0] != null) {
                Log.e("xtunnel", "深度重连失败: " + err[0].getMessage(), err[0]);
                return;
            }
            Log.i("xtunnel", "深度重连[4/5] 已在主线程完成重建");
        } finally {
            /* 4. 无论成败都要给看门狗解开闸门并复位基线（endRebuild 内部会清
                  failStreak / lastBytes，效果覆盖原来的 notifySwitched）。
                  注意这里**不清退避窗口** —— 退避只由"探测真的通了"来清，
                  否则连续几次僵死会把间隔推到 10 分钟，网络早好了也得干等。 */
            TunnelWatchdog.endRebuild();
            Log.i("xtunnel", "深度重连[5/5] 结束，看门狗已恢复判定");
        }
    }

    public void stopService() {
        /* 先停看门狗：避免它在我们主动断开的过程中又触发一次深度重连。 */
        try { TunnelWatchdog.stop(); } catch (Throwable t) { }
        try { stopDiagnosticsLogger(); } catch (Throwable t) { }
        try { stopForeground(true); } catch (Throwable t) { }
        try { Tunnel.stopSocksProxy(); } catch (Exception e) { }
        try { TProxyStopService(); } catch (Throwable t) { }
        if (tunFd != null) {
            try { tunFd.close(); } catch (IOException e) {}
            tunFd = null;
        }
        System.exit(0);
    }

    /* 诊断日志线程句柄。原来这个函数写好了却从未被调用，
       隧道僵死时连一份现场都没有；现在由 startService() 启动。
       注意必须幂等 —— startService() 在深度重连/热切换里会被反复调用。 */
    private volatile Thread diagThread = null;

    /**
     * 诊断日志：把 Go 侧写的 xtunnel_status.log 转发到 logcat。
     *
     * 原实现是每 5 秒把整份状态**无条件**打印一遍 —— 真启动起来 logcat 会被
     * 重复内容糊死，反而把看门狗（TAG=TunnelWatchdog）的关键日志挤没了。
     * 所以这里只在内容**发生变化**时打印，既留现场又不刷屏。
     */
    private void startDiagnosticsLogger() {
        if (diagThread != null && diagThread.isAlive()) {
            return;
        }
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                String last = null;
                while (!Thread.currentThread().isInterrupted()) {
                    try { Thread.sleep(5000); } catch (InterruptedException e) { break; }
                    try {
                        // 直接读取 Go 侧写入的诊断文件(不依赖 gomobile 方法绑定)
                        java.io.File f = new java.io.File(getFilesDir(), "xtunnel_status.log");
                        if (!f.exists()) {
                            continue;
                        }
                        java.io.BufferedReader r = new java.io.BufferedReader(new java.io.FileReader(f));
                        StringBuilder sb = new StringBuilder();
                        String line;
                        while ((line = r.readLine()) != null) { sb.append(line).append("\n"); }
                        r.close();
                        String cur = sb.toString();
                        /* 状态没变就不打 —— 否则每 5 秒刷一遍同样的内容毫无意义。 */
                        if (!cur.equals(last)) {
                            last = cur;
                            Log.i("xtunnel", "===== TUNNEL STATUS =====\n" + cur);
                        }
                    } catch (Throwable t) {
                        Log.e("xtunnel", "diag error", t);
                    }
                }
            }
        }, "xtunnel-diag");
        diagThread = t;
        t.start();
    }

    private void stopDiagnosticsLogger() {
        Thread t = diagThread;
        diagThread = null;
        if (t != null) {
            t.interrupt();
        }
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
		int flags = PendingIntent.FLAG_UPDATE_CURRENT;
		if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
			flags |= PendingIntent.FLAG_IMMUTABLE;
		}
		PendingIntent pi = PendingIntent.getActivity(this, 0, i, flags);
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
        int flags = PendingIntent.FLAG_UPDATE_CURRENT;
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            flags |= PendingIntent.FLAG_IMMUTABLE;
        }
        PendingIntent pi = PendingIntent.getActivity(this, 0, i, flags);
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
