package com.x.tunnel;

import android.content.Context;
import android.content.pm.PackageManager;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.os.Build;
import android.os.Handler;
import android.os.Looper;
import android.telephony.SubscriptionInfo;
import android.telephony.SubscriptionManager;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 本地可用 IP 池（移动端三网络分区版）
 *
 * 设计要点（与桌面端两层架构对齐，但针对手机的多运营商异构网络做了扩展）：
 *   1. 手机可能处于三种截然不同的网络：WiFi（家里/公司可能还是不同运营商）、卡槽1 数据、卡槽2 数据。
 *      因此在同一运营商下测出的"黄金 IP"，换一条线路很可能完全不通。
 *      => 把 IP 池拆成 3 个独立分区：wifi / sim1 / sim2，每区上限 50 个。
 *   2. 更新池子时【只更新当前所在网络对应的那一个分区】，互不污染。
 *      => 在 WiFi 下更新只动 wifi 池；切到卡槽 2 更新只动 sim2 池。
 *   3. 演进合并规则（老豆定式，按 50 容量等比换算）：
 *      已有旧池 -> 保留旧池最前面的 10 个（历史稳定 IP）+ 本次新扫出的最优 40 个（去重后装满 50）；
 *      首次无池 -> 直接取本次新扫出的最优 50 个。
 *   4. 优选时（{@link CfOptimizer}）把三个分区合并去重后统一打分，从中挑 Top N（N = 节点并发数）。
 *      => 从 WiFi 走进电梯切 5G 的瞬间，优选引擎可以直接在合并池里秒选当前线路能通的最快 IP。
 */
public class CfIpPool {

    private static final String TAG = "CfIpPool";

    public static final String SLOT_WIFI = "wifi";
    public static final String SLOT_SIM1 = "sim1";
    public static final String SLOT_SIM2 = "sim2";

    /** 单分区容量上限 */
    public static final int POOL_CAPACITY = 50;
    /** 更新时保留旧池头部数量（稳定性锚点） */
    private static final int KEEP_OLD = 10;
    /** 期望新扫出的 IP 数量 */
    private static final int SCAN_TARGET_NEW = 40;

    /* 扫描参数：手机上兼顾速度与耗电，单次随机采样 1200 个子网 + 64 路并发 */
    private static final int SCAN_MAX_SUBNETS = 1200;
    private static final int SCAN_CONCURRENCY = 64;
    private static final int SCAN_TIMEOUT_MS = 700;
    private static final int PROBE_PORT = 443;

    private static final String ASSET_SUBNETS = "cf_subnets.txt";

    private static final AtomicBoolean scanning = new AtomicBoolean(false);
    private static final Handler mainHandler = new Handler(Looper.getMainLooper());

    /* ======================= 回调 ======================= */

    public interface PoolCallback {
        /** 进度：done/total 为探测进度，hint 为阶段说明（可选实现） */
        default void onProgress(int done, int total, String hint) { }

        /** 完成：success=false 时 message 说明失败原因 */
        void onFinish(boolean success, String slot, int poolSize, String message);
    }

    /* ======================= 网络分区识别 ======================= */

    /** 检测当前所处的网络分区键（wifi / sim1 / sim2）。 */
    public static String detectSlot(Context context) {
        try {
            ConnectivityManager cm = (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
            if (cm == null) return SLOT_WIFI;
            Network active = cm.getActiveNetwork();
            NetworkCapabilities caps = active == null ? null : cm.getNetworkCapabilities(active);

            /* 注意：VPN 建立后 getActiveNetwork() 返回的是 VPN 网络，
               它只带 TRANSPORT_VPN，无法据此判断底层是 WiFi 还是哪张卡。
               此时代理进程正在跑，必须回溯到真实底层承载网络。 */
            if (caps == null || caps.hasTransport(NetworkCapabilities.TRANSPORT_VPN)) {
                NetPick pick = findUnderlying(cm);
                caps = (pick == null) ? null : pick.caps;
            }
            if (caps == null) return SLOT_WIFI;

            if (caps.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR)) {
                return detectCellularSlot(context);
            }
            // WiFi / 以太网 / 其他一律归入 wifi 分区（同一出口线路即同一分区）
            return SLOT_WIFI;
        } catch (Throwable t) {
            Log.w(TAG, "detectSlot 失败: " + t.getMessage());
            return SLOT_WIFI;
        }
    }

    /**
     * 取当前真实底层承载网络（非 VPN），供探测 socket 绑定使用；
     * VPN 在线时探测若走隧道，测到的就不是本地线路质量了。
     */
    public static Network getUnderlyingNetwork(Context context) {
        try {
            ConnectivityManager cm = (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
            if (cm == null) return null;
            Network active = cm.getActiveNetwork();
            NetworkCapabilities caps = active == null ? null : cm.getNetworkCapabilities(active);
            if (caps != null && !caps.hasTransport(NetworkCapabilities.TRANSPORT_VPN)) {
                return active;
            }
            NetPick pick = findUnderlying(cm);
            return (pick == null) ? null : pick.network;
        } catch (Throwable t) {
            return null;
        }
    }

    private static class NetPick {
        Network network;
        NetworkCapabilities caps;
    }

    /**
     * 在 VPN 在线时回溯真实底层承载：优先 WiFi（系统默认优先无线局域网），否则取蜂窝。
     */
    private static NetPick findUnderlying(ConnectivityManager cm) {
        NetPick wifi = null;
        NetPick cellular = null;
        NetPick other = null;
        try {
            Network[] all = cm.getAllNetworks();
            if (all == null) return null;
            for (Network n : all) {
                NetworkCapabilities c = cm.getNetworkCapabilities(n);
                if (c == null) continue;
                if (c.hasTransport(NetworkCapabilities.TRANSPORT_VPN)) continue;
                if (!c.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)) continue;
                NetPick pick = new NetPick();
                pick.network = n;
                pick.caps = c;
                if (c.hasTransport(NetworkCapabilities.TRANSPORT_WIFI)) {
                    wifi = pick;
                } else if (c.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR)) {
                    cellular = pick;
                } else if (other == null) {
                    other = pick;
                }
            }
        } catch (Throwable t) {
            Log.w(TAG, "回溯底层网络失败: " + t.getMessage());
        }
        if (wifi != null) return wifi;
        if (cellular != null) return cellular;
        return other;
    }

    private static String detectCellularSlot(Context context) {
        int slotIndex = -1;
        int subId = SubscriptionManager.INVALID_SUBSCRIPTION_ID;
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
                subId = SubscriptionManager.getDefaultDataSubscriptionId();
            }
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP_MR1 && subId != SubscriptionManager.INVALID_SUBSCRIPTION_ID) {
                SubscriptionManager sm = (SubscriptionManager) context.getSystemService(Context.TELEPHONY_SUBSCRIPTION_SERVICE);
                if (sm != null && hasPhoneStatePermission(context)) {
                    SubscriptionInfo info = sm.getActiveSubscriptionInfo(subId);
                    if (info != null) slotIndex = info.getSimSlotIndex();
                }
            }
        } catch (Throwable t) {
            Log.w(TAG, "读取卡槽信息失败: " + t.getMessage());
        }

        if (slotIndex >= 0) {
            return slotIndex >= 1 ? SLOT_SIM2 : SLOT_SIM1;
        }
        // 无权限兜底：用 subId 奇偶做稳定映射，两张卡的 subId 通常相邻，可稳定落到不同分区
        if (subId != SubscriptionManager.INVALID_SUBSCRIPTION_ID) {
            return (subId % 2 == 0) ? SLOT_SIM2 : SLOT_SIM1;
        }
        return SLOT_SIM1;
    }

    private static boolean hasPhoneStatePermission(Context context) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M) return false;
        try {
            return context.checkSelfPermission(android.Manifest.permission.READ_PHONE_STATE)
                    == PackageManager.PERMISSION_GRANTED;
        } catch (Throwable t) {
            return false;
        }
    }

    /** 分区展示名称，例如「WiFi」「卡槽1 · 中国移动」 */
    public static String slotLabel(Context context, String slot) {
        if (SLOT_WIFI.equals(slot)) return "WiFi";
        String operator = detectOperatorName(context, slot);
        String base = SLOT_SIM2.equals(slot) ? "卡槽2" : "卡槽1";
        return TextUtils.isEmpty(operator) ? base : (base + " · " + operator);
    }

    private static String detectOperatorName(Context context, String slot) {
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP_MR1 && hasPhoneStatePermission(context)) {
                SubscriptionManager sm = (SubscriptionManager) context.getSystemService(Context.TELEPHONY_SUBSCRIPTION_SERVICE);
                if (sm != null) {
                    int wantSlot = SLOT_SIM2.equals(slot) ? 1 : 0;
                    List<SubscriptionInfo> list = sm.getActiveSubscriptionInfoList();
                    if (list != null) {
                        for (SubscriptionInfo info : list) {
                            if (info.getSimSlotIndex() == wantSlot) {
                                CharSequence name = info.getCarrierName();
                                if (name != null) return name.toString();
                            }
                        }
                    }
                }
            }
            if (SLOT_SIM1.equals(slot)) {
                TelephonyManager tm = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
                if (tm != null) return tm.getSimOperatorName();
            }
        } catch (Throwable ignored) {
        }
        return "";
    }

    public static String slotDisplayName(String slot) {
        if (SLOT_WIFI.equals(slot)) return "WiFi";
        if (SLOT_SIM2.equals(slot)) return "卡槽2";
        return "卡槽1";
    }

    /* ======================= 池读取 / 合并 ======================= */

    public static List<String> parseCsv(String csv) {
        List<String> out = new ArrayList<>();
        if (TextUtils.isEmpty(csv)) return out;
        for (String s : csv.split(",")) {
            String v = s.trim();
            if (v.length() > 0 && !out.contains(v)) out.add(v);
        }
        return out;
    }

    public static String joinCsv(List<String> ips) {
        if (ips == null || ips.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (String ip : ips) {
            if (sb.length() > 0) sb.append(',');
            sb.append(ip);
        }
        return sb.toString();
    }

    /** 某分区当前 IP 数量 */
    public static int poolSize(Preferences prefs, String slot) {
        return parseCsv(prefs.getCfPool(slot)).size();
    }

    /**
     * 合并三分区为优选候选池：当前所在分区优先，其余分区随后，去重。
     * 这样即便刚切网、当前分区还没更新过，也能立刻复用另外两条线路积累的 IP。
     */
    public static List<String> getMergedPool(Context context, Preferences prefs) {
        String current = detectSlot(context);
        LinkedHashSet<String> merged = new LinkedHashSet<>();
        merged.addAll(parseCsv(prefs.getCfPool(current)));
        for (String slot : new String[]{SLOT_WIFI, SLOT_SIM1, SLOT_SIM2}) {
            if (!slot.equals(current)) {
                merged.addAll(parseCsv(prefs.getCfPool(slot)));
            }
        }
        List<String> out = new ArrayList<>(merged);
        Log.i(TAG, "合并候选池: 当前分区=" + current + " 合计 " + out.size() + " 个");
        return out;
    }

    /** 三分区总量（去重后），用于界面统计 */
    public static int mergedSize(Context context, Preferences prefs) {
        return getMergedPool(context, prefs).size();
    }

    /* ======================= 池更新（扫描） ======================= */

    public interface ProgressCallback {
        void onProgress(int done, int total);
    }

    /**
     * 异步更新【当前网络分区】的 IP 池。
     *
     * @param context  上下文
     * @param manual   是否用户手动触发（影响日志与提示文案）
     * @param callback 进度与完成回调（主线程）
     */
    public static void updateCurrentPoolAsync(final Context context, final boolean manual, final PoolCallback callback) {
        if (!scanning.compareAndSet(false, true)) {
            Log.w(TAG, "IP 池扫描已在运行中，忽略重复请求");
            if (callback != null) {
                mainHandler.post(() -> callback.onFinish(false, detectSlot(context), 0, "扫描正在进行中"));
            }
            return;
        }

        final Context appCtx = context.getApplicationContext();
        new Thread(new Runnable() {
            @Override
            public void run() {
                boolean ok = false;
                String slot = SLOT_WIFI;
                int size = 0;
                String message = "";
                try {
                    slot = detectSlot(appCtx);
                    Preferences prefs = new Preferences(appCtx);
                    String slotName = slotDisplayName(slot);

                    List<String> oldPool = parseCsv(prefs.getCfPool(slot));
                    Log.i(TAG, "开始更新 " + slotName + " IP 池（旧池 " + oldPool.size() + " 个）");

                    // 1. 准备候选：旧池全部 + 随机采样自网点
                    List<String> candidates = new ArrayList<>(oldPool);
                    List<String> sampled = sampleSubnetIps(appCtx, SCAN_MAX_SUBNETS);
                    for (String ip : sampled) {
                        if (!candidates.contains(ip)) candidates.add(ip);
                    }
                    final int total = candidates.size();
                    Log.i(TAG, "候选 " + total + " 个（含旧池 " + oldPool.size() + " 个）");

                    // 2. 并发探测（绑定底层真实网络，避免 VPN 隧道干扰测量）
                    Network bindNet = getUnderlyingNetwork(appCtx);
                    List<IpLatency> alive = probeAll(candidates, total, bindNet, callback);

                    if (alive.isEmpty()) {
                        message = "未探测到可用 IP，请检查当前网络连通性";
                        Log.w(TAG, message);
                    } else {
                        // 3. 合并演进：旧池前 KEEP_OLD + 新扫最优，去重装满 POOL_CAPACITY
                        List<String> merged = mergePool(oldPool, alive);
                        String nowStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault()).format(new Date());
                        String dateStr = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault()).format(new Date());

                        prefs.setCfPool(slot, joinCsv(merged));
                        prefs.setCfPoolTime(slot, nowStr);
                        prefs.setCfPoolDate(slot, dateStr);

                        size = merged.size();
                        ok = true;
                        message = "已更新 " + slotName + " IP 池，共 " + size + " 个可用 IP";
                        Log.i(TAG, message + "（旧池保留 " + Math.min(KEEP_OLD, oldPool.size())
                                + " 个 + 新增 " + Math.max(0, size - Math.min(KEEP_OLD, oldPool.size())) + " 个）");
                    }
                } catch (Throwable t) {
                    message = "IP 池更新异常: " + t.getMessage();
                    Log.e(TAG, message, t);
                } finally {
                    scanning.set(false);
                }

                final boolean fOk = ok;
                final String fSlot = slot;
                final int fSize = size;
                final String fMsg = message;
                if (callback != null) {
                    mainHandler.post(() -> callback.onFinish(fOk, fSlot, fSize, fMsg));
                }
            }
        }, "cf-ip-pool-scanner").start();
    }

    /**
     * 每天首次进入时，若当前分区今天还没更新过，则静默在后台更新一次。
     * 只更新当前分区，避免一次性跑三次扫描耗电。
     */
    public static void maybeDailyUpdate(final Context context) {
        maybeDailyUpdate(context, null);
    }

    public static void maybeDailyUpdate(final Context context, final PoolCallback callback) {
        try {
            final Context appCtx = context.getApplicationContext();
            final Preferences prefs = new Preferences(appCtx);
            final String slot = detectSlot(appCtx);
            String today = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault()).format(new Date());
            if (today.equals(prefs.getCfPoolDate(slot))) {
                Log.i(TAG, slotDisplayName(slot) + " IP 池今日已更新，跳过自动更新");
                return;
            }
            Log.i(TAG, slotDisplayName(slot) + " IP 池今日未更新，后台自动更新...");
            updateCurrentPoolAsync(appCtx, false, callback);
        } catch (Throwable t) {
            Log.w(TAG, "每日自动更新检查失败: " + t.getMessage());
        }
    }

    public static boolean isScanning() {
        return scanning.get();
    }

    /**
     * 合并算法：旧池头部 KEEP_OLD 个（历史稳定锚点）优先保留，其余名额用本次新测出的最优 IP 补满。
     */
    static List<String> mergePool(List<String> oldPool, List<IpLatency> alive) {
        LinkedHashSet<String> result = new LinkedHashSet<>();

        int keep = Math.min(KEEP_OLD, oldPool.size());
        for (int i = 0; i < keep; i++) {
            result.add(oldPool.get(i));
        }

        // alive 已按延迟升序
        for (IpLatency item : alive) {
            if (result.size() >= POOL_CAPACITY) break;
            result.add(item.ip);
        }

        // 极端情况兜底：新扫结果太少时，用旧池剩余项补齐
        for (String ip : oldPool) {
            if (result.size() >= POOL_CAPACITY) break;
            result.add(ip);
        }

        return new ArrayList<>(result);
    }

    /* ======================= 探测实现 ======================= */

    static class IpLatency {
        final String ip;
        final float latency;

        IpLatency(String ip, float latency) {
            this.ip = ip;
            this.latency = latency;
        }
    }

    private static List<IpLatency> probeAll(List<String> candidates, final int total,
                                            final Network bindNet, final PoolCallback callback) {
        final List<IpLatency> alive = Collections.synchronizedList(new ArrayList<IpLatency>());
        final int threads = Math.min(SCAN_CONCURRENCY, Math.max(1, candidates.size()));
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        final CountDownLatch latch = new CountDownLatch(candidates.size());
        final java.util.concurrent.atomic.AtomicInteger done = new java.util.concurrent.atomic.AtomicInteger(0);
        final long[] lastNotify = new long[]{0L};

        try {
            for (final String ip : candidates) {
                pool.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            float lat = CfOptimizer.probeTcpHandshake(ip, PROBE_PORT, SCAN_TIMEOUT_MS, bindNet);
                            if (lat > 0.0f) {
                                alive.add(new IpLatency(ip, lat));
                            }
                        } finally {
                            int d = done.incrementAndGet();
                            long now = System.currentTimeMillis();
                            if (callback != null && (now - lastNotify[0] > 300 || d == total)) {
                                lastNotify[0] = now;
                                final int fd = d;
                                mainHandler.post(() -> callback.onProgress(fd, total, "正在探测可用 IP..."));
                            }
                            latch.countDown();
                        }
                    }
                });
            }
            // 全量探测上限：最坏情况每轮 RTT 超时 × 轮数
            long waitMs = (long) Math.ceil((double) candidates.size() / threads) * (SCAN_TIMEOUT_MS + 120L) + 3000L;
            latch.await(Math.min(waitMs, 90000L), TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            Log.w(TAG, "探测中断: " + t.getMessage());
        } finally {
            pool.shutdownNow();
        }

        Collections.sort(alive, new Comparator<IpLatency>() {
            @Override
            public int compare(IpLatency a, IpLatency b) {
                return Float.compare(a.latency, b.latency);
            }
        });
        Log.i(TAG, "探测结束：候选 " + candidates.size() + " 个，可用 " + alive.size() + " 个");
        return alive;
    }

    /**
     * 从内置 Cloudflare 全网段清单中随机采样，每个 /24 段生成 1 个随机主机 IP。
     * 采样而非全扫，是为了让单次扫描在手机上 10~20 秒内完成；配合"每天更新 + 旧池保留"，
     * 覆盖面会随天数自然扩散到全量网段。
     */
    private static List<String> sampleSubnetIps(Context context, int maxCount) {
        List<String> subnets = loadSubnets(context);
        if (subnets.isEmpty()) {
            Log.w(TAG, "子网清单为空，退回内置种子 IP");
            return new ArrayList<>();
        }
        Collections.shuffle(subnets, new Random(System.currentTimeMillis()));
        int take = Math.min(maxCount, subnets.size());
        Random rnd = new Random(System.currentTimeMillis() ^ 0x5DEECE66DL);
        List<String> ips = new ArrayList<>(take);
        for (int i = 0; i < take; i++) {
            String ip = randomIpInSubnet(subnets.get(i), rnd);
            if (ip != null) ips.add(ip);
        }
        return ips;
    }

    private static String randomIpInSubnet(String cidr, Random rnd) {
        try {
            int slash = cidr.indexOf('/');
            String base = slash > 0 ? cidr.substring(0, slash) : cidr;
            String[] parts = base.split("\\.");
            if (parts.length != 4) return null;
            int a = Integer.parseInt(parts[0]);
            int b = Integer.parseInt(parts[1]);
            int c = Integer.parseInt(parts[2]);
            // /24 段：末位取 1~254 随机（避开网络号与广播地址）
            int d = 1 + rnd.nextInt(254);
            return a + "." + b + "." + c + "." + d;
        } catch (Throwable t) {
            return null;
        }
    }

    private static List<String> loadSubnets(Context context) {
        List<String> out = new ArrayList<>();
        InputStream is = null;
        BufferedReader br = null;
        try {
            is = context.getAssets().open(ASSET_SUBNETS);
            br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String line;
            while ((line = br.readLine()) != null) {
                String v = line.trim();
                if (v.isEmpty() || v.startsWith("#")) continue;
                if (v.contains("/")) out.add(v);
            }
        } catch (Throwable t) {
            Log.w(TAG, "读取子网清单失败: " + t.getMessage());
        } finally {
            try { if (br != null) br.close(); } catch (Throwable ignored) {}
            try { if (is != null) is.close(); } catch (Throwable ignored) {}
        }
        Log.i(TAG, "加载 Cloudflare 子网清单: " + out.size() + " 条");
        return out;
    }
}
