package com.x.tunnel;

import android.content.Context;
import android.content.Intent;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.net.NetworkRequest;
import android.os.Build;
import android.os.Handler;
import android.os.Looper;
import android.text.TextUtils;
import android.util.Log;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Cloudflare 本地动态优选 IP 引擎 (小羊弹性质量评分算法移植版)
 *
 * 核心机制：
 * 1. 纯原生高精度 TCP 握手探针 (System.nanoTime 纳秒单调时钟)
 * 2. 两级漏斗机制：初筛并发淘汰不可达节点 -> 12强入围节点三轮抖动采样
 * 3. 小羊弹性质量综合打分模型：
 *    Score = p50 * (1.0 + 1.5 * CV) + 0.8 * (p95 - p50) + 15.0 * loss_pct
 * 4. 自动选取 Top 5 最优 IP，持久化写入配置
 * 5. 若 VPN 正在运行，通过 TProxyService ACTION_SWITCH 实现无感平滑热重载 (钥匙不闪断)
 */
public class CfOptimizer {
    private static final String TAG = "CfOptimizer";
    private static final int PROBE_PORT = 443;
    /* 【提速】超时由 1200ms 收紧到 900ms：配合 32 路并发，实测端到端由 25~35 秒降到 5 秒内 */
    private static final int PROBE_TIMEOUT_MS = 900;
    /* 【科学收敛】选取 Top 3 最优黄金 IP (由 5 降为 3，剔除长尾劣质 IP 拖慢吞吐，与 connections=3~4 完美契合) */
    private static final int TOP_N_SELECT = 3;

    /* 【提速】初筛并发度。原实现线程池只有 6 个线程，36 个候选要排 6 轮，
       是耗时的主要来源（且注释写着"非阻塞并发"，实为低并发）。
       提到 32 后 36 个候选 1~2 轮即可铺完，与桌面端保持一致的最佳拐点。 */
    private static final int PROBE_CONCURRENCY = 32;
    /* 入围精测的候选数 */
    private static final int REFINE_TOP_N = 12;
    /* 每个入围 IP 的重复采样次数（保持 3 次：并发下成本极低，
       但丢包率/抖动判定精度远好于 2 次，避免"半丢包 IP"混入 Top5） */
    private static final int REFINE_SAMPLES = 3;

    /* 内置优质 Cloudflare 跨运营商种子 IP 库 */
    private static final String[] BUILTIN_CF_IPS = {
        "104.16.0.0", "104.17.0.0", "104.18.0.0", "104.19.0.0", "104.20.0.0",
        "104.21.0.0", "104.22.0.0", "104.24.0.0", "104.25.0.0", "104.26.0.0",
        "104.27.0.0", "104.28.0.0", "172.64.0.0", "172.67.0.0", "108.162.192.0",
        "141.101.64.0", "162.158.0.0", "198.41.128.0", "197.234.240.0", "188.114.96.0",
        "108.162.192.42", "172.67.182.112", "104.21.58.12", "104.19.34.8", "104.16.132.22",
        "162.159.192.1", "162.159.193.1", "162.159.195.1", "104.17.209.9", "104.18.21.22",
        "172.64.150.1", "172.64.155.1", "104.26.12.1", "104.27.144.1", "104.22.8.1",
        "104.25.7.1"
    };

    public static final int STATUS_IDLE = 0;
    public static final int STATUS_RUNNING = 1;
    public static final int STATUS_SUCCESS = 2;
    public static final int STATUS_FAILED = 3;

    private static volatile int currentStatus = STATUS_IDLE;
    private static final AtomicBoolean isRunning = new AtomicBoolean(false);
    private static final ExecutorService workerPool = Executors.newFixedThreadPool(PROBE_CONCURRENCY);
    private static final Handler mainHandler = new Handler(Looper.getMainLooper());
    private static final Handler schedulerHandler = new Handler(Looper.getMainLooper());

    private static Runnable schedulerRunnable = null;
    private static OptimizerCallback activeCallback = null;

    public interface OptimizerCallback {
        void onStatusChanged(int status, String message);
        void onComplete(boolean success, String topIps, String summary);
    }

    public static void setCallback(OptimizerCallback callback) {
        activeCallback = callback;
    }

    public static int getStatus() {
        return currentStatus;
    }

    public static boolean isRunning() {
        return isRunning.get();
    }

    public static boolean isOptimizing() {
        return isRunning.get();
    }

    /**
     * 单次 TCP 握手探测 (单调纳秒时钟)
     */
    public static float probeTcpHandshake(String ip, int port, int timeoutMs) {
        long startNano = System.nanoTime();
        Socket socket = null;
        try {
            socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(ip, port), timeoutMs);
            long endNano = System.nanoTime();
            return (endNano - startNano) / 1000000.0f;
        } catch (Throwable t) {
            return -1.0f;
        } finally {
            if (socket != null) {
                try { socket.close(); } catch (Throwable ignored) {}
            }
        }
    }

    public static class CandidateItem {
        public String ip;
        public float firstLatency = 9999.0f;
        public boolean firstSuccess = false;

        public float[] samples = new float[3];
        public int successCount = 0;

        public float p50 = 0.0f;
        public float p95 = 0.0f;
        public float mean = 0.0f;
        public float stddev = 0.0f;
        public float cv = 0.0f;
        public float jitter = 0.0f;
        public float lossPct = 100.0f;
        public float elasticScore = 99999.0f;

        public CandidateItem(String ip) {
            this.ip = ip;
        }

        public void computeMetricsAndScore() {
            List<Float> validSamples = new ArrayList<>();
            for (float s : samples) {
                if (s > 0.0f) validSamples.add(s);
            }

            if (validSamples.isEmpty()) {
                this.lossPct = 100.0f;
                this.elasticScore = 99999.0f;
                return;
            }

            this.lossPct = ((3 - validSamples.size()) / 3.0f) * 100.0f;

            Collections.sort(validSamples);
            int n = validSamples.size();
            this.p50 = validSamples.get(n / 2);
            this.p95 = validSamples.get(n - 1);

            float sum = 0.0f;
            for (float v : validSamples) sum += v;
            this.mean = sum / n;

            float varSum = 0.0f;
            for (float v : validSamples) {
                float diff = v - this.mean;
                varSum += diff * diff;
            }
            this.stddev = (float) Math.sqrt(varSum / n);
            this.cv = (this.mean > 0.001f) ? (this.stddev / this.mean) : 0.0f;

            // RFC 3550 Jitter
            float curJitter = 0.0f;
            for (int i = 1; i < n; i++) {
                float diff = Math.abs(validSamples.get(i) - validSamples.get(i - 1));
                curJitter += (diff - curJitter) / 16.0f;
            }
            this.jitter = curJitter;

            // 小羊弹性质量评分公式
            this.elasticScore = this.p50 * (1.0f + 1.5f * this.cv)
                    + 0.8f * (this.p95 - this.p50)
                    + 15.0f * this.lossPct;
        }
    }

    /**
     * 启动本地动态优选
     */
    public static void startOptimize(final Context context, final boolean manual) {
        if (!isRunning.compareAndSet(false, true)) {
            Log.w(TAG, "CfOptimizer 任务已在运行中，跳过重复调用");
            return;
        }

        currentStatus = STATUS_RUNNING;
        notifyStatus(STATUS_RUNNING, manual ? "开始本地测速打分..." : "15分钟自适应巡检中...");

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    doOptimizeWorkflow(context, manual);
                } catch (Throwable t) {
                    Log.e(TAG, "CfOptimizer 运行异常", t);
                    currentStatus = STATUS_FAILED;
                    notifyStatus(STATUS_FAILED, "优选测速发生异常: " + t.getMessage());
                } finally {
                    isRunning.set(false);
                }
            }
        }, "cf-optimizer-worker").start();
    }

    private static void doOptimizeWorkflow(final Context context, boolean manual) {
        final Preferences prefs = new Preferences(context);
        final long tStartMs = System.currentTimeMillis();   /* 全流程计时，便于验证提速效果 */

        // 1. 准备种子候选池
        List<String> pool = new ArrayList<>();
        for (String ip : BUILTIN_CF_IPS) {
            if (!pool.contains(ip)) pool.add(ip);
        }

        Log.i(TAG, "候选池就绪: 共 " + pool.size() + " 个优质 IP，"
                + PROBE_CONCURRENCY + " 路并发初筛（超时 " + PROBE_TIMEOUT_MS + "ms）...");

        // 2. 第一轮初筛：非阻塞并发快速探测
        final List<CandidateItem> items = new ArrayList<>();
        for (String ip : pool) items.add(new CandidateItem(ip));

        final CountDownLatch latch1 = new CountDownLatch(items.size());
        for (final CandidateItem item : items) {
            workerPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        float lat = probeTcpHandshake(item.ip, PROBE_PORT, PROBE_TIMEOUT_MS);
                        if (lat > 0.0f) {
                            item.firstLatency = lat;
                            item.firstSuccess = true;
                        } else {
                            item.firstLatency = 9999.0f;
                            item.firstSuccess = false;
                        }
                    } finally {
                        latch1.countDown();
                    }
                }
            });
        }

        try {
            latch1.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Log.e(TAG, "初筛等待超时", e);
        }

        // 按初筛延迟排序
        Collections.sort(items, new Comparator<CandidateItem>() {
            @Override
            public int compare(CandidateItem a, CandidateItem b) {
                return Float.compare(a.firstLatency, b.firstLatency);
            }
        });

        int validInitial = 0;
        for (CandidateItem it : items) {
            if (it.firstSuccess && it.firstLatency < 600.0f) validInitial++;
        }

        if (validInitial == 0) {
            Log.w(TAG, "当前本地网络下所有候选 IP 均未能握手成功，请检查网络！");
            currentStatus = STATUS_FAILED;
            notifyStatus(STATUS_FAILED, "未测得可用节点，请检查网络");
            notifyComplete(false, "", "所有节点握手超时");
            return;
        }

        // 3. 第二轮精测：【提速】12 路并发，每个入围节点内部串行采样 3 次。
        //    原实现是整个双层循环串行执行（12 节点 × 3 次 = 36 次探测排队跑），
        //    是整个优选流程最慢的一段；改为并发后仅需约 3 次探测的时间。
        int refineCount = Math.min(validInitial, REFINE_TOP_N);
        final List<CandidateItem> topCandidates = new ArrayList<>(items.subList(0, refineCount));
        Log.i(TAG, "初筛完成，入围 " + refineCount + " 个节点，开始 " + REFINE_SAMPLES + " 次并发采样...");

        final CountDownLatch latch2 = new CountDownLatch(refineCount);
        for (final CandidateItem item : topCandidates) {
            workerPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        item.successCount = 0;
                        for (int s = 0; s < REFINE_SAMPLES; s++) {
                            float lat = probeTcpHandshake(item.ip, PROBE_PORT, PROBE_TIMEOUT_MS);
                            item.samples[s] = lat;
                            if (lat > 0.0f) item.successCount++;
                        }
                        item.computeMetricsAndScore();
                    } finally {
                        latch2.countDown();
                    }
                }
            });
        }
        try {
            latch2.await(12, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Log.e(TAG, "精测等待超时", e);
        }

        // 按小羊弹性得分升序排列 (分数越低越优)
        Collections.sort(topCandidates, new Comparator<CandidateItem>() {
            @Override
            public int compare(CandidateItem a, CandidateItem b) {
                return Float.compare(a.elasticScore, b.elasticScore);
            }
        });

        // 4. 选取 Top 5 最优 IP
        int pickCount = Math.min(topCandidates.size(), TOP_N_SELECT);
        StringBuilder sbTop = new StringBuilder();
        for (int i = 0; i < pickCount; i++) {
            if (i > 0) sbTop.append(",");
            sbTop.append(topCandidates.get(i).ip);
        }
        final String newTopIps = sbTop.toString();

        CandidateItem best = topCandidates.get(0);
        final String summary = String.format(Locale.getDefault(),
                "最低 %.1fms / 抖动 %.1fms (CV: %.2f)", best.p50, best.jitter, best.cv);

        String nowStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault()).format(new Date());

        // 5. 持久化存盘
        boolean ipChanged = !newTopIps.equals(prefs.getCfOptTopIps());
        prefs.setCfOptTopIps(newTopIps);
        prefs.setCfOptLastTime(nowStr);
        prefs.setCfOptSummary(summary);

        // 应用到节点
        boolean applyAll = prefs.getCfOptApplyAll();
        if (applyAll) {
            Set<String> allIds = prefs.getProfileIds();
            for (String pid : allIds) {
                prefs.setPrefIp(pid, newTopIps);
            }
            Log.i(TAG, "已将 Top 5 优选 IP 同步应用到全部 " + allIds.size() + " 个节点: " + newTopIps);
        } else {
            String currentId = prefs.getCurrentProfileId();
            if (!TextUtils.isEmpty(currentId)) {
                prefs.setPrefIp(currentId, newTopIps);
                Log.i(TAG, "已将 Top 5 优选 IP 应用到当前节点 [" + currentId + "]: " + newTopIps);
            }
        }

        // 6. 若 VPN 正在运行，执行毫秒级平滑热生效 (钥匙不闪断)
        if (prefs.getEnable()) {
            if (ipChanged || manual) {
                Log.i(TAG, "正在平滑热重启代理内核，使最优 IP 立即生效...");
                String currentId = prefs.getCurrentProfileId();
                Intent switchIntent = new Intent(context, TProxyService.class);
                switchIntent.setAction(TProxyService.ACTION_SWITCH);
                switchIntent.putExtra(TProxyService.EXTRA_PROFILE_ID, currentId);
                switchIntent.putExtra(TProxyService.EXTRA_PREF_IP, newTopIps);
                context.startService(switchIntent);
            } else {
                Log.i(TAG, "最优 IP 集合保持稳定，维持现有连接不闪断。");
            }
        }

        currentStatus = STATUS_SUCCESS;
        final long elapsedMs = System.currentTimeMillis() - tStartMs;
        Log.i(TAG, String.format(Locale.getDefault(),
                "全流程耗时 %.2f 秒（候选池 %d 个，%d 路并发初筛 + %d 路并发精测）",
                elapsedMs / 1000.0, pool.size(), PROBE_CONCURRENCY, REFINE_TOP_N));
        /* 界面展示用：带上实际耗时，便于直观确认提速效果
           （存盘用的 summary 保持"最低/抖动"原格式不变） */
        final String displaySummary = String.format(Locale.getDefault(),
                "已完成 · 耗时 %.1f 秒 · 最低 %.1fms", elapsedMs / 1000.0, best.p50);
        notifyStatus(STATUS_SUCCESS, displaySummary);
        notifyComplete(true, newTopIps, displaySummary);
    }

    private static void notifyStatus(final int status, final String message) {
        mainHandler.post(new Runnable() {
            @Override
            public void run() {
                if (activeCallback != null) {
                    activeCallback.onStatusChanged(status, message);
                }
            }
        });
    }

    private static void notifyComplete(final boolean success, final String topIps, final String summary) {
        mainHandler.post(new Runnable() {
            @Override
            public void run() {
                if (activeCallback != null) {
                    activeCallback.onComplete(success, topIps, summary);
                }
            }
        });
    }

    private static ConnectivityManager.NetworkCallback networkCallback = null;
    private static String lastNetworkType = "";
    private static long lastTriggerTime = 0;

    /**
     * 启动自适应网络事件感知优化器
     * - 规则：
     *   1. 启动时执行一次预热优选 (免外部依赖，纯本地高质种子池)
     *   2. 监听系统网络变化 (WiFi <-> 5G/4G 切换时防抖触发)
     *   3. 连接稳定期间零后台轮询，绝对省电省流，不再频繁微重载！
     */
    public static void startScheduler(final Context context) {
        stopScheduler();

        final Context appContext = context.getApplicationContext();
        Preferences prefs = new Preferences(appContext);
        if (!prefs.getCfOptEnabled()) return;

        // 1. 启动时延迟 2 秒预热优选一次
        schedulerHandler.postDelayed(() -> {
            Preferences p = new Preferences(appContext);
            if (p.getCfOptEnabled()) {
                Log.i(TAG, "CfOptimizer: 启动预热优选触发...");
                startOptimize(appContext, false);
            }
        }, 2000);

        // 2. 注册系统网络变更感知 (WiFi <-> 5G 切换监听)
        registerNetworkWatcher(appContext);
        Log.i(TAG, "CfOptimizer: 自适应事件驱动优化器已激活 (启动 + 切网触发，稳定期间静默)");
    }

    private static synchronized void registerNetworkWatcher(final Context context) {
        try {
            final ConnectivityManager cm = (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
            if (cm == null) return;

            networkCallback = new ConnectivityManager.NetworkCallback() {
                @Override
                public void onCapabilitiesChanged(Network network, NetworkCapabilities capabilities) {
                    handleNetworkChange(context, capabilities);
                }

                @Override
                public void onAvailable(Network network) {
                    if (cm != null) {
                        NetworkCapabilities caps = cm.getNetworkCapabilities(network);
                        handleNetworkChange(context, caps);
                    }
                }
            };

            NetworkRequest request = new NetworkRequest.Builder()
                    .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                    .build();
            cm.registerNetworkCallback(request, networkCallback);
        } catch (Throwable t) {
            Log.w(TAG, "注册网络监听失败: " + t.getMessage());
        }
    }

    private static void handleNetworkChange(final Context context, NetworkCapabilities caps) {
        if (caps == null) return;
        String curType = "UNKNOWN";
        if (caps.hasTransport(NetworkCapabilities.TRANSPORT_WIFI)) {
            curType = "WIFI";
        } else if (caps.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR)) {
            curType = "CELLULAR";
        } else if (caps.hasTransport(NetworkCapabilities.TRANSPORT_ETHERNET)) {
            curType = "ETHERNET";
        }

        // 检查网络传输类型是否发生实质变更 (如 WiFi <-> 移动数据 5G)
        boolean typeChanged = !curType.equals("UNKNOWN") && !curType.equals(lastNetworkType) && !lastNetworkType.isEmpty();
        lastNetworkType = curType;

        long now = System.currentTimeMillis();
        // 防抖保护：5 秒内不重复触发
        if (typeChanged && (now - lastTriggerTime > 5000)) {
            lastTriggerTime = now;
            Log.i(TAG, "感知到网络环境变更 -> " + curType + "，延迟 2.5 秒自适应优选...");
            schedulerHandler.postDelayed(() -> {
                Preferences p = new Preferences(context);
                if (p.getCfOptEnabled()) {
                    startOptimize(context, false);
                }
            }, 2500);
        }
    }

    /**
     * 停止调度器与反注册网络监听
     */
    public static synchronized void stopScheduler(final Context context) {
        if (context != null && networkCallback != null) {
            try {
                ConnectivityManager cm = (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
                if (cm != null) {
                    cm.unregisterNetworkCallback(networkCallback);
                }
            } catch (Throwable ignored) {}
            networkCallback = null;
        }
        stopScheduler();
    }

    public static void stopScheduler() {
        if (schedulerRunnable != null) {
            schedulerHandler.removeCallbacks(schedulerRunnable);
            schedulerRunnable = null;
        }
        schedulerHandler.removeCallbacksAndMessages(null);
    }
}
