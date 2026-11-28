package com.x.tunnel;

import android.content.Context;
import android.content.Intent;
import android.os.Handler;
import android.os.HandlerThread;
import android.util.Log;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 隧道自愈看门狗（v2.10.11）
 *
 * ======================== 为什么需要它 ========================
 * 老豆实测的故障：安卓端跑一段时间后无法代理上网，**切换线路也没用**，
 * 只有把 App 整个关掉再打开才恢复。
 *
 * 这条「切线路也没用」的信息量极大 —— 切换节点只重启 Go 侧的 SOCKS5 + WS
 * 连接池（{@link TProxyService#switchProfile}），**完全不动 TUN 网卡和原生
 * 转发层（hev-socks5-tunnel 的 lwIP 栈）**。既然切换无效，病灶就在切换碰不到
 * 的那一层：原生转发层/TUN 卡住了，而 Go 内核看起来一切正常。
 *
 * 更糟的是这个 App 之前**没有任何健康巡检**：
 *   - TProxyGetStats() 声明了却从未调用；
 *   - startDiagnosticsLogger() 写好了却从未启动。
 * 于是隧道僵死时界面照样显示「已连接」，既不会喊也不会自救。
 *
 * ======================== 怎么判（三路取证，避免误判） ========================
 * 只靠单一信号一定会误杀（比如刚好在电梯里、地铁过隧道）。所以每轮同时看三样：
 *
 *   A. 隧道连通性：{@link ServerTester#testCurrentVpnConnectivity()}
 *      真正发 HTTP 请求要 204，这是"到底能不能上网"的唯一硬证据。
 *   B. 内核心跳：读 Go 侧每 5 秒刷新的 xtunnel_health.txt，
 *      超过 {@link #HEARTBEAT_STALE_MS} 没更新 = Go 协程已停。
 *   C. 原生层流量：TProxyGetStats() 的收发字节数是否还在涨。
 *
 * 判定规则（宁可漏判，绝不误杀）：
 *   - 连通性 OK            → 一切正常，清零计数；
 *   - 连通性失败但字节数在涨 → 可能只是目标站被墙/慢，判「疑似」，不重连；
 *   - 连通性失败 + 字节数不涨 + 内核心跳正常 → 计入僵死计数；
 *   - 连续 {@link #FAIL_THRESHOLD} 轮僵死 → 判定隧道卡死，执行深度重连。
 *
 * ======================== 怎么救（等价于关掉重开，但不杀进程） ========================
 * 关键前提（已核实源码 native_stop_service）：原生层 quit + pthread_join 后会
 * 复位 is_working，**同一进程内可以安全地停止再启动**。所以能做得比"杀进程"更优雅：
 *
 *   1. 关掉 TUN（tunFd.close）
 *   2. 停原生转发层（TProxyStopService）
 *   3. 停 Go 内核（Tunnel.stopSocksProxy）
 *   4. 等一下让 fd/端口真正释放
 *   5. 重新 establish TUN + 重启原生层 + 重连内核
 *
 * 这套流程与手动关掉 App 再打开在效果上等价，但用户是无感的：
 * 通知栏不消失、不需要重新授权 VPN、正在听的播客断几秒就自己接上。
 *
 * 注意：连续重连失败时会指数退避（{@link #BACKOFF_BASE_MS} 翻倍，最多
 * {@link #BACKOFF_MAX_MS}），避免网络真的断掉时疯狂折腾把电量耗光。
 */
public final class TunnelWatchdog {

    private static final String TAG = "TunnelWatchdog";

    /* 巡检间隔。30 秒是权衡：更短纯属浪费电（真断了用户也不会 10 秒内就发现），
       更长则体感"卡了半天才恢复"。 */
    private static final long CHECK_INTERVAL_MS = 30 * 1000;

    /* 连续多少轮判定失败才动手。3 轮 ≈ 90 秒 —— 足够过滤掉地铁过隧道、
       电梯里信号短暂消失这类正常抖动，又不会让用户卡太久。 */
    private static final int FAIL_THRESHOLD = 3;

    /* ★ v2.10.19：「单站问题」豁免最多连续几轮。
       切网后的半开长连接，Go 心跳照刷、原生字节照涨，会一直命中豁免而永远不重连
       （朋友反馈「要开飞行模式再开网络才能上油管」的第二个帮凶）。
       3 轮 = 90 秒后强制进入僵死判定，让深度重连有机会救回来。 */
    private static final int EXEMPT_MAX_ROUNDS = 3;

    /* Go 侧心跳文件超过这么久没刷新，就认为 Go 协程已经死了。
       Go 是每 5 秒刷一次，90 秒 = 给了 18 次机会，绝不会误判。 */
    private static final long HEARTBEAT_STALE_MS = 90 * 1000;

    /* ★ v2.10.25：长睡唤醒检测（老豆实测「睡一晚起来假活，切线路/停止启动都没用，
       只有完全杀进程重开才行」）。
       Doze / 厂商深度睡眠会把整个进程冻结 —— 连看门狗自己的 Handler 定时都停摆，
       所以不能用定时器计时。唯一可靠的信号是「墙钟差」：冻结期间
       System.currentTimeMillis() 照走，而 postDelayed 不走。正常巡检每 30 秒一跳，
       墙钟差 > 1 小时必然是睡过。 */
    private static final long SLEEP_WAKE_THRESHOLD_MS = 60 * 60 * 1000;
    /* 上次巡检的墙钟时间。用 static 内存即可：进程冻结时值保留、唤醒后可比对；
       进程被真杀掉重启后自动归零，不会把「刚启动」误判成长睡。 */
    private static long lastWallTickMs = 0;
    /* 长睡唤醒首轮探测不通 → 挂起等下一轮二次确认，避免刚唤醒网络还没就绪时误杀。 */
    private static boolean wakeConfirmPending = false;

    /* 重连退避：第 1 次立即，之后 1 分钟、2 分钟、4 分钟…最多 10 分钟。
       网络真的没了的时候，疯狂重连只会把电耗光。 */
    private static final long BACKOFF_BASE_MS = 60 * 1000;
    private static final long BACKOFF_MAX_MS = 10 * 60 * 1000;

    private static final String HEALTH_FILE = "xtunnel_health.txt";

    /* ---------- 单例状态 ---------- */
    private static HandlerThread thread;
    private static Handler handler;
    private static volatile boolean running = false;
    private static Context appContext;

    /* 连续僵死轮数 */
    private static int failStreak = 0;
    /* 连续被判「单站问题」的轮数（v2.10.19，超过上限强制重连） */
    private static int exemptStreak = 0;
    /* 连续重连失败次数（用于退避） */
    private static int healAttempts = 0;
    /* 上一次看到的原生层收发总字节（判断"还在不在转发"） */
    private static long lastBytes = -1;
    /* 下一次允许重连的时间点 */
    private static long nextHealAt = 0;

    private static final AtomicBoolean healing = new AtomicBoolean(false);

    /**
     * 重建中标志：深度重连期间（拆 + 重建 + 等握手）看门狗必须闭嘴。
     * 否则会出现很荒谬的场面 —— 刚拆掉 TUN 还没建好，看门狗一探不通，
     * 又判定一次僵死，再触发一次重建，来回打架。
     */
    private static volatile boolean rebuilding = false;

    /** 由 Service 在深度重连开始时调用。 */
    public static void beginRebuild() {
        rebuilding = true;
        Log.i(TAG, "深度重连进行中，看门狗暂停判定");
    }

    /** 由 Service 在深度重连结束时调用。 */
    public static void endRebuild() {
        rebuilding = false;
        failStreak = 0;
        exemptStreak = 0;
        lastBytes = -1;
        Log.i(TAG, "深度重连结束，看门狗恢复判定");
    }

    /* 累计统计，供设置页/日志查看（万一复发，老豆能一眼看到发生过几次） */
    private static volatile int totalHeals = 0;
    private static volatile long lastHealTime = 0;
    private static volatile String lastHealReason = "";

    private TunnelWatchdog() {
    }

    /* ======================== 对外接口 ======================== */

    /** 启动看门狗（幂等）。必须在 VPN 已建立后调用。 */
    public static synchronized void start(Context context) {
        if (running) {
            return;
        }
        appContext = context.getApplicationContext();
        thread = new HandlerThread("pat-watchdog");
        thread.start();
        handler = new Handler(thread.getLooper());

        running = true;
        failStreak = 0;
        healAttempts = 0;
        lastBytes = -1;
        nextHealAt = 0;
        /* v2.10.25：启动即锚定墙钟。冷重启后新进程第一次巡检差值是几秒，不会误判长睡。 */
        lastWallTickMs = System.currentTimeMillis();
        wakeConfirmPending = false;

        handler.postDelayed(tick, CHECK_INTERVAL_MS);
        Log.i(TAG, "自愈看门狗已启动（每 " + (CHECK_INTERVAL_MS / 1000) + " 秒巡检一次，连续 "
                + FAIL_THRESHOLD + " 轮僵死即深度重连）");
    }

    /** 停止看门狗（VPN 断开时调用）。 */
    public static synchronized void stop() {
        running = false;
        if (handler != null) {
            handler.removeCallbacksAndMessages(null);
            handler = null;
        }
        if (thread != null) {
            thread.quitSafely();
            thread = null;
        }
        failStreak = 0;
        healAttempts = 0;
        lastBytes = -1;
        wakeConfirmPending = false;
        lastWallTickMs = 0;
        Log.i(TAG, "自愈看门狗已停止");
    }

    /** 热切换节点后调用：切换本身已验证可用，重置计数避免把"刚切换"误判成僵死。 */
    public static void notifySwitched() {
        failStreak = 0;
        lastBytes = -1;      /* 新连接池的字节基线与旧的不同，重置避免误判 */
        Log.i(TAG, "节点已切换，看门狗计数重置");
    }

    /**
     * 隧道确认可用后调用：连退避窗口一起清零。
     *
     * 必须和 {@link #notifySwitched()} 分开 —— 切换节点只说明"换了个节点"，
     * 并不证明网络好了；而重建成功后如果探测真的通了，才有资格把退避归零。
     * 否则连续几次僵死会把退避推到 10 分钟，之后即便网络早好了，
     * 下次再卡死也要干等 10 分钟才能自愈。
     */
    private static void resetBackoff() {
        healAttempts = 0;
        nextHealAt = 0;
    }

    /** 供界面显示：累计自愈次数。 */
    public static int getTotalHeals() {
        return totalHeals;
    }

    /** 供界面显示：最近一次自愈原因。 */
    public static String getLastHealReason() {
        return lastHealReason;
    }

    /* ======================== 巡检主循环 ======================== */

    private static final Runnable tick = new Runnable() {
        @Override
        public void run() {
            if (!running) {
                return;
            }
            try {
                checkOnce(detectSleepWake());
            } catch (Throwable t) {
                /* 看门狗自己绝不能崩 —— 崩了就再没人自愈了。 */
                Log.e(TAG, "巡检异常（已忽略，继续下一轮）", t);
            }
            if (running && handler != null) {
                handler.postDelayed(this, CHECK_INTERVAL_MS);
            }
        }
    };

    /**
     * 每轮巡检先算墙钟差。返回 true 表示距上次巡检超过 1 小时（进程被冻结过）。
     * 无论是否判为长睡，都刷新锚点 —— 这样只有"刚解冻的第一轮"会返回 true。
     */
    private static boolean detectSleepWake() {
        long now = System.currentTimeMillis();
        boolean woke = false;
        if (lastWallTickMs > 0) {
            long gap = now - lastWallTickMs;
            if (gap >= SLEEP_WAKE_THRESHOLD_MS) {
                woke = true;
                Log.w(TAG, "检测到长睡唤醒，墙钟间隔 " + (gap / 60000) + " 分钟");
            }
        }
        lastWallTickMs = now;
        return woke;
    }

    private static void checkOnce(boolean wokeFromSleep) {
        /* 深度重连进行中：这时候 TUN 刚被拆掉/正在重建，探测必然不通 ——
           要是还继续判定，就会"拆→探不通→再判僵死→再拆"来回打架。
           直接跳过本轮，等 Service 那边 endRebuild() 之后再恢复。 */
        if (rebuilding) {
            return;
        }

        Preferences prefs = new Preferences(appContext);
        /* 用户主动关掉 VPN 了，就别管了 */
        if (!prefs.getEnable()) {
            return;
        }

        /* ---- 证据 A：真实连通性（唯一硬证据） ---- */
        int latency = ServerTester.testCurrentVpnConnectivity();
        if (latency > 0) {
            if (wakeConfirmPending) {
                Log.i(TAG, "长睡唤醒后二次探测已通（" + latency + "ms），取消冷重启");
                wakeConfirmPending = false;
            }
            if (failStreak > 0) {
                Log.i(TAG, "隧道已恢复正常（延迟 " + latency + "ms），僵死计数清零");
            }
            failStreak = 0;
            exemptStreak = 0;
            /* 真的通了才允许把退避窗口归零 —— 否则连续几次僵死会把退避推到
               10 分钟，之后即便网络早恢复，下次卡死也要干等 10 分钟。 */
            resetBackoff();
            lastBytes = -1;
            return;
        }

        /* ---- ★ v2.10.25 长睡唤醒专路径（先探测再决定，两次确认防误判） ----
           老豆实测：睡一晚起来 PAT 显示在运行但上不了网，切线路/停止启动都没用，
           只有完全杀进程重开才行 —— 同进程 HEAL 在这种场景下救不回来，必须换进程。
           这里不依赖心跳/字节数（冻结刚恢复时它们看着像活着），只认真实连通性探测：
             唤醒首轮不通 → 标记挂起，等 30 秒后第二轮再探；
             第二轮仍不通 → 发起进程级冷重启（真杀进程 + 闹钟自拉起）；
             任一轮通了 → 取消，什么都不做（避免刚解冻网络还没就绪时误杀）。 */
        if (wokeFromSleep && !wakeConfirmPending) {
            wakeConfirmPending = true;
            Log.w(TAG, "长睡唤醒首轮探测不通，标记二次确认，30 秒后复查（暂不重启）");
            return;
        }
        if (!wokeFromSleep && wakeConfirmPending) {
            wakeConfirmPending = false;
            Log.w(TAG, "长睡唤醒第二轮探测仍不通 → 发起进程级冷重启");
            requestColdRestart("长睡唤醒连续两次探测不通");
            return;
        }

        /* ---- 证据 B：Go 内核心跳 ----
           心跳缺失/过期 = Go 那个刷文件的协程已经停了（整个 Go 运行时卡死或退出）。
           另有一个更硬的信号：心跳文件里"池子存在=0"，说明 echPool 已被置 nil
           且没有重建 —— 内核实际已经不在服务了，这时即使文件还很新也要重连。 */
        long[] hb = readHeartbeat();
        long hbAge = hb[0];
        boolean poolGone = (hb[1] == 0L);
        boolean heartbeatDead = (hbAge < 0) || (hbAge > HEARTBEAT_STALE_MS) || poolGone;

        /* ---- 证据 C：原生层是否还在转发 ---- */
        long bytes = readNativeBytes();
        boolean bytesGrowing = (lastBytes >= 0 && bytes > lastBytes);
        lastBytes = bytes;

        if (bytesGrowing && !heartbeatDead) {
            /* 原生层还在干活、内核也活着，只是这个目标站不通
               （被墙 / 该站慢 / DNS 抽风）。多数时候确实不该重连。
               ★ 但「心跳活 + 字节涨」也可能是切网后的半开长连接：
                 短连接探测一直失败、长视频流量早断了，字节只是残留包在涨。
                 所以豁免不能无限期 —— 连续 3 轮（90 秒）就强制进入僵死判定。 */
            exemptStreak++;
            if (exemptStreak < EXEMPT_MAX_ROUNDS) {
                Log.i(TAG, "连通性探测失败，但原生层仍在转发（bytes " + bytes
                        + "），暂判单站问题，不重连（" + exemptStreak + "/"
                        + EXEMPT_MAX_ROUNDS + "）");
                return;
            }
            Log.w(TAG, "连续 " + exemptStreak + " 轮被判单站问题但连通性始终未恢复"
                    + " —— 疑似切网后半开长连接，强制进入僵死判定");
        } else {
            exemptStreak = 0;
        }

        failStreak++;
        Log.w(TAG, "连通性探测失败（连续 " + failStreak + "/" + FAIL_THRESHOLD
                + " 轮，内核心跳 " + (hbAge < 0 ? "缺失" : hbAge + "ms 前")
                + (poolGone ? " [池子已空]" : (hbAge > HEARTBEAT_STALE_MS ? " [已过期]" : ""))
                + "，原生字节 " + bytes + "）");

        if (failStreak < FAIL_THRESHOLD) {
            return;
        }

        /* ---- 判定僵死，准备深度重连 ---- */
        long now = System.currentTimeMillis();
        if (now < nextHealAt) {
            Log.i(TAG, "判定僵死，但仍在退避窗口内（还需 "
                    + ((nextHealAt - now) / 1000) + " 秒），跳过本轮");
            return;
        }

        failStreak = 0;
        exemptStreak = 0;
        doDeepHeal(hbAge, bytes);
    }

    /* ======================== 深度重连 ======================== */

    private static void doDeepHeal(long hbAge, long bytes) {
        if (!healing.compareAndSet(false, true)) {
            return;
        }
        try {
            String reason = "连通性连续失败" + FAIL_THRESHOLD + "轮"
                    + "，内核心跳" + (hbAge < 0 ? "缺失" : hbAge + "ms前")
                    + "，原生字节停止增长(" + bytes + ")";
            Log.w(TAG, "===== 判定隧道僵死，执行深度重连 =====");
            Log.w(TAG, "原因: " + reason);

            Preferences prefs = new Preferences(appContext);
            String profileId = prefs.getCurrentProfileId();
            if (profileId == null || profileId.isEmpty()) {
                Log.e(TAG, "没有当前节点，无法重连");
                return;
            }

            /* 交给 Service 做"关 TUN + 停原生层 + 停内核 + 重建"的完整流程。
               比起在这里拼装 Intent 参数，让 Service 从 prefs 自己读更不容易漏项。 */
            Intent intent = new Intent(appContext, TProxyService.class);
            intent.setAction(TProxyService.ACTION_HEAL);
            intent.putExtra(TProxyService.EXTRA_PROFILE_ID, profileId);
            appContext.startService(intent);

            totalHeals++;
            lastHealTime = System.currentTimeMillis();
            lastHealReason = reason;

            /* 指数退避：1min → 2min → 4min → … → 最多 10min */
            healAttempts++;
            long backoff = BACKOFF_BASE_MS << Math.min(healAttempts - 1, 4);
            if (backoff > BACKOFF_MAX_MS) {
                backoff = BACKOFF_MAX_MS;
            }
            nextHealAt = System.currentTimeMillis() + backoff;
            Log.i(TAG, "深度重连已触发（累计第 " + totalHeals + " 次），下次最短间隔 "
                    + (backoff / 1000) + " 秒");

        } catch (Throwable t) {
            Log.e(TAG, "深度重连触发失败", t);
        } finally {
            healing.set(false);
        }
    }

    /**
     * ★ v2.10.25 进程级冷重启（区别于 doDeepHeal 的同进程重建）。
     *
     * 为什么不用同进程 HEAL：老豆实测长睡唤醒后 wss/TUN 半死，同进程拆建救不回来，
     * 只有把进程彻底杀掉重开才行 —— native  lwIP 栈冻结恢复后可能有无法在同进程
     * 复位的残留状态。所以这里让 Service 走「拆隧道 → 设闹钟 → System.exit(0)」，
     * 新进程由 ServiceReceiver 收到闹钟后拉起，是一张白纸的全新启动。
     *
     * 用独立的 coldRestarting 闸，避免和已有 healing(同进程) 互相干扰。
     */
    private static final AtomicBoolean coldRestarting = new AtomicBoolean(false);

    private static void requestColdRestart(String reason) {
        if (!coldRestarting.compareAndSet(false, true)) {
            Log.i(TAG, "已有冷重启在进行中，忽略本次请求");
            return;
        }
        try {
            Preferences prefs = new Preferences(appContext);
            if (!prefs.getEnable()) {
                Log.i(TAG, "冷重启取消：用户已关闭 VPN");
                return;
            }
            /* 先让看门狗闭嘴：马上要拆 TUN + 杀进程，避免这几秒里又判一次僵死。
               进程会被杀，static 标志随之归零，新进程不需要手动 endRebuild()。 */
            beginRebuild();
            Intent intent = new Intent(appContext, TProxyService.class);
            intent.setAction(TProxyService.ACTION_COLD_RESTART);
            appContext.startService(intent);

            totalHeals++;
            lastHealTime = System.currentTimeMillis();
            lastHealReason = "冷重启:" + reason;
            Log.w(TAG, "===== 已发起进程级冷重启: " + reason + " =====");
        } catch (Throwable t) {
            Log.e(TAG, "冷重启发起失败", t);
        } finally {
            /* 只清自己的闸；healing 是同进程 HEAL 的闸，别串味。
               进程马上会被杀，static 全部归零，这里其实清不清都会重置。 */
            coldRestarting.set(false);
        }
    }

    /* ======================== 取证辅助 ======================== */

    /**
     * 读 Go 侧心跳文件，返回 age（毫秒）与"池子是否还在"。
     * 返回 -1 表示文件缺失或读不出来（此时视为心跳已死）。
     *
     * 文件格式（由 Go 侧 diagFileFlusher 每 5 秒写）：
     *   <unix_ms>|<就绪通道>/<总通道>|<池子存在1/0>|<监听存在1/0>
     */
    private static long[] readHeartbeat() {
        /* [0] = age 毫秒；[1] = 池子是否存在(1/0)，未知时 -1 */
        long[] out = new long[]{-1L, -1L};
        FileInputStream in = null;
        try {
            File f = new File(appContext.getFilesDir(), HEALTH_FILE);
            if (!f.exists()) {
                return out;
            }
            in = new FileInputStream(f);
            byte[] buf = new byte[256];
            int n = in.read(buf);
            if (n <= 0) {
                return out;
            }
            String s = new String(buf, 0, n, "UTF-8").trim();
            String[] parts = s.split("\\|");
            if (parts.length < 3) {
                return out;
            }
            long ts = Long.parseLong(parts[0].trim());
            long age = System.currentTimeMillis() - ts;
            out[0] = age < 0 ? 0 : age;
            out[1] = "1".equals(parts[2].trim()) ? 1L : 0L;
            return out;
        } catch (Throwable t) {
            return out;
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Throwable ignored) {
                }
            }
        }
    }

    /**
     * 读原生层累计收发字节数（tx + rx）。
     * 这个数字一直涨说明 lwIP 还在正常转发，只是某个目标站不通 —— 不该重连。
     * 拿不到统计时返回 -1（老版本 native 库可能没有该符号）。
     */
    private static long readNativeBytes() {
        try {
            long[] st = TProxyService.nativeStats();
            if (st == null || st.length < 4) {
                return -1;
            }
            /* native 返回顺序：[tx_packets, tx_bytes, rx_packets, rx_bytes] */
            return st[1] + st[3];
        } catch (Throwable t) {
            return -1;
        }
    }
}
