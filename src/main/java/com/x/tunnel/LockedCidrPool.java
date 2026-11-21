package com.x.tunnel;

import android.content.Context;
import android.net.Network;
import android.util.Log;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 锁定段池（老豆 2026-11-22 定式）——「不再猜落地，而是锁死验证过好用的段」。
 *
 * 设计：
 *   1. 种子段硬编码（老豆实测：电信=10段落NRT、移动=7段落HKG），永不清空。
 *   2. 每槽（wifi/sim1/sim2）各存一份自己攒的 colo 段集，落地验证命中的 /24 记一笔，
 *      攒够 N 次才纳入——各地自扫自用，一台错了不影响全局。
 *   3. 手动选中运营商（电信/移动）后【无视落地检测】，直接在锁定段内枚举全部 IP
 *      纯测 TCP 握手延迟、按快到慢取前 N。不查 colo（避免"有几天落地查不到"）。
 *   4. 一键恢复出厂：清掉自攒段集与偏好，回到种子段。
 *
 * 依据：锁定段集把搜索空间从 6534 段几万 IP 缩到 2560/1792，并发粗测 15~30 秒出结果。
 */
public final class LockedCidrPool {

    private static final String TAG = "LockedCidrPool";

    /** 手动选定某运营商后硬锁的段集（老豆 2026-11-22 提供的实测清单） */
    private static final String[] TELECOM_SEEDS = {
            "162.159.39.0/24", "172.64.52.0/24", "162.159.38.0/24",
            "162.159.45.0/24", "162.159.44.0/24", "104.17.145.0/24",
            "104.21.95.0/24", "104.19.39.0/24", "104.21.86.0/24", "108.162.198.0/24"
    };
    private static final String[] MOBILE_SEEDS = {
            "104.21.89.0/24", "104.25.246.0/24", "104.25.244.0/24",
            "104.25.253.0/24", "104.25.250.0/24", "172.64.53.0/24", "104.25.245.0/24"
    };

    /** 运营商偏好值（与 Preferences.SLOT_CARRIER_* 对应） */
    public static final String CARRIER_AUTO = "auto";
    public static final String CARRIER_TELECOM = "telecom";
    public static final String CARRIER_MOBILE = "mobile";
    public static final String CARRIER_OTHER = "other";

    /** 粗测参数：2560 个 IP / 128 并发 / 700ms 超时 → 理论最坏 ~14 秒 */
    private static final int PROBE_CONCURRENCY = 128;
    private static final int PROBE_TIMEOUT_MS = 700;
    private static final int POOL_TARGET = 200;
    /** 落地命中累计到该次数才把一个 /24 纳入自攒段集（防偶发漂移） */
    private static final int COLO_CONFIRM_TIMES = 3;

    private LockedCidrPool() { }

    /** 是否该走锁定段优选：槽位开关打开且手动选了电信/移动 */
    public static boolean shouldLock(Preferences prefs, String slot) {
        if (!prefs.getSlotLockEnable(slot)) return false;
        String c = prefs.getSlotCarrier(slot);
        return CARRIER_TELECOM.equals(c) || CARRIER_MOBILE.equals(c);
    }

    /** 按运营商给出种子段（去重） */
    public static List<String> seedCidrs(String carrier) {
        LinkedHashSet<String> out = new LinkedHashSet<String>();
        if (CARRIER_TELECOM.equals(carrier)) {
            Collections.addAll(out, TELECOM_SEEDS);
        } else if (CARRIER_MOBILE.equals(carrier)) {
            Collections.addAll(out, MOBILE_SEEDS);
        } else {
            Collections.addAll(out, TELECOM_SEEDS);
            Collections.addAll(out, MOBILE_SEEDS);
        }
        return new ArrayList<String>(out);
    }

    /** 种子段 + 该槽自攒段集合并（自攒的排后面） */
    public static List<String> effectiveCidrs(Preferences prefs, String slot, String carrier) {
        LinkedHashSet<String> out = new LinkedHashSet<String>(seedCidrs(carrier));
        String saved = prefs.getSlotColoCidrs(slot);
        if (saved != null && !saved.trim().isEmpty()) {
            for (String c : saved.split(",")) {
                c = c.trim();
                if (!c.isEmpty()) out.add(c);
            }
        }
        return new ArrayList<String>(out);
    }

    /**
     * 从锁定段枚举全部 IP，纯 TCP 握手测速，按延迟排序取前 target 个。
     * 绑定底层网络（VPN 在线时不绑会测到隧道质量）。
     *
     * @return 按快到慢排序的 IP 列表；一个都探不到则返回空（调用方降级回广撒）
     */
    public static List<String> probeLockedFast(final List<String> cidrs, final Network bindNet,
                                               final int target,
                                               final CfIpPool.PoolCallback callback) {
        final long t0 = System.currentTimeMillis();
        final List<String> ips = new ArrayList<String>();
        for (String cidr : cidrs) {
            addAllIpsOfSubnet(cidr, ips);
        }
        if (ips.isEmpty()) return new ArrayList<String>();

        final Object[] lat = new Object[ips.size()]; // Float, 与 ips 同下标
        final AtomicInteger done = new AtomicInteger(0);
        final int total = ips.size();
        ExecutorService ex = Executors.newFixedThreadPool(Math.min(PROBE_CONCURRENCY, total));
        final CountDownLatch latch = new CountDownLatch(total);
        for (int i = 0; i < total; i++) {
            final int idx = i;
            final String ip = ips.get(i);
            ex.execute(new Runnable() {
                @Override public void run() {
                    try {
                        float ms = CfOptimizer.probeTcpHandshake(ip, CfIpPool.PROBE_PORT, PROBE_TIMEOUT_MS, bindNet);
                        if (ms > 0) {
                            lat[idx] = Float.valueOf(ms);
                        }
                    } catch (Throwable ignored) {
                    } finally {
                        int d = done.incrementAndGet();
                        if (callback != null && (d % 200 == 0 || d == total)) {
                            callback.onProgress(d, total, "锁定段测速");
                        }
                        latch.countDown();
                    }
                }
            });
        }
        try { latch.await(60, TimeUnit.SECONDS); } catch (Throwable ignored) { }
        ex.shutdownNow();

        // 按延迟排序存活 IP：对索引排序，避免 O(n²) 的 indexOf 查找
        final Object[] latRef = lat;
        List<Integer> idxs = new ArrayList<Integer>();
        for (int i = 0; i < total; i++) {
            if (lat[i] != null) idxs.add(Integer.valueOf(i));
        }
        Collections.sort(idxs, new Comparator<Integer>() {
            @Override public int compare(Integer a, Integer b) {
                return Float.compare(((Float) latRef[a.intValue()]).floatValue(),
                                     ((Float) latRef[b.intValue()]).floatValue());
            }
        });
        List<String> sorted = new ArrayList<String>();
        for (Integer idxObj : idxs) sorted.add(ips.get(idxObj.intValue()));
        if (sorted.size() > target) sorted = new ArrayList<String>(sorted.subList(0, target));
        Log.i(TAG, String.format(Locale.getDefault(),
                "锁定段穷举完成：%d 段 -> %d IP 探测 -> %d 存活 -> 取前 %d，耗时 %dms",
                cidrs.size(), total, sorted.size(), Math.min(target, sorted.size()),
                System.currentTimeMillis() - t0));
        return sorted;
    }

    /**
     * 记录一次落地命中：colo 属目标区域时给该 /24 计数，攒够 COLO_CONFIRM_TIMES 次纳入自攒段集。
     * 每槽独立，只增不减。返回是否新纳入了段。
     */
    public static boolean recordColoHit(Context ctx, Preferences prefs, String slot,
                                        String ip, String colo, String targetCarrier) {
        if (ip == null || colo == null) return false;
        // 判断落地是否属目标运营商区域
        boolean hit;
        if (CARRIER_TELECOM.equals(targetCarrier)) {
            hit = colo.startsWith("NRT") || colo.equals("KIX") || colo.equals("FUK") || colo.equals("OKA");
        } else if (CARRIER_MOBILE.equals(targetCarrier)) {
            hit = colo.equals("HKG");
        } else {
            hit = colo.equals("HKG") || colo.startsWith("NRT") || colo.equals("KIX");
        }
        if (!hit) return false;

        String cidr = cidr24(ip);
        if (cidr == null) return false;
        String countKey = "ColoHit_" + slot + "_" + cidr.replace('.', '_');
        android.content.SharedPreferences sp = ctx.getSharedPreferences(Preferences.PREFS_NAME, Context.MODE_MULTI_PROCESS);
        int n = sp.getInt(countKey, 0) + 1;
        sp.edit().putInt(countKey, n).commit();
        if (n < COLO_CONFIRM_TIMES) return false;

        Set<String> cur = new LinkedHashSet<String>();
        String saved = prefs.getSlotColoCidrs(slot);
        if (saved != null && !saved.trim().isEmpty()) {
            for (String c : saved.split(",")) if (!c.trim().isEmpty()) cur.add(c.trim());
        }
        if (!cur.add(cidr)) return false; // 已在自攒集
        prefs.setSlotColoCidrs(slot, join(cur));
        Log.i(TAG, "自攒段纳入：slot=" + slot + " cidr=" + cidr + " (落地 " + colo + " 命中 " + n + " 次)");
        return true;
    }

    /** 把 /24 段展开成全部 256 个主机 IP（跳过网络号 .0 和广播 .255） */
    private static void addAllIpsOfSubnet(String cidr, List<String> out) {
        try {
            String prefix;
            if (cidr.contains("/")) {
                String[] p = cidr.split("/");
                int bits = Integer.parseInt(p[1]);
                if (bits != 24) return; // 只支持 /24，老豆给的清单全是 /24
                String[] oct = p[0].split("\\.");
                if (oct.length != 4) return;
                prefix = oct[0] + "." + oct[1] + "." + oct[2] + ".";
            } else {
                String[] oct = cidr.split("\\.");
                if (oct.length != 4) return;
                prefix = oct[0] + "." + oct[1] + "." + oct[2] + ".";
            }
            for (int i = 1; i <= 254; i++) out.add(prefix + i);
        } catch (Throwable ignored) { }
    }

    private static String cidr24(String ip) {
        int i = ip.lastIndexOf('.');
        return i < 0 ? null : ip.substring(0, i) + ".0/24";
    }

    private static String join(Set<String> s) {
        StringBuilder b = new StringBuilder();
        for (String x : s) {
            if (b.length() > 0) b.append(',');
            b.append(x);
        }
        return b.toString();
    }
}
