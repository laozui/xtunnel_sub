package com.x.tunnel;

import android.content.Context;
import android.net.Network;
import android.text.TextUtils;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 段池（CIDR Pool）—— 把「IP 池」换成「/24 段池」。
 *
 * 为什么换（2026-11-10 电信线路实测）：
 *   26136 个样本里只有 107 个段落日/港/新，其中日本只有 4 个段
 *   （108.162.198 / 172.64.52 / 172.64.53 / 172.64.229），香港 0 个段。
 *   也就是说 200 个 IP 的池子其实只有 4 个段可选 —— 池子是伪抽象。
 *
 *   而同一个 /24 段内落点高度一致（实测 .1/.2/.50/.100/.200/.254 全落 NRT），
 *   所以「段」才是稳定且经济的单位：数量少两个数量级，排名还更稳。
 *
 * 两级优选（约 2~5 秒）：
 *   Stage 1  每段测 1 个代表 IP → 段级 colo + 延迟排名
 *   Stage 2  取最快 3 段，每段抽 10 个 IP 精测 → 排序取 TopN
 */
public class CfCidrLib {
    private static final String TAG = "CfCidrLib";

    private static final int STAGE1_THREADS = 24;
    private static final int STAGE1_MAX = 360;
    private static final int STAGE2_DEEP_CIDR = 3;
    private static final int STAGE2_PER_CIDR = 10;

    static class Entry {
        String cidr;
        String colo;
        String area;
        float rttMs;
        String ip;
    }

    static File libFile(Context ctx, String slot) {
        return new File(ctx.getFilesDir(), "cf_cidr_" + slot + ".json");
    }

    public static boolean hasLib(Context ctx, String slot) {
        File f = libFile(ctx, slot);
        return f.exists() && f.length() > 64;
    }

    public static int libCount(Context ctx, String slot) {
        return load(ctx, slot).size();
    }

    static List<Entry> load(Context ctx, String slot) {
        List<Entry> out = new ArrayList<>();
        File f = libFile(ctx, slot);
        if (!f.exists()) return out;
        FileInputStream in = null;
        try {
            in = new FileInputStream(f);
            byte[] buf = new byte[(int) f.length()];
            int off = 0, n;
            while (off < buf.length && (n = in.read(buf, off, buf.length - off)) > 0) off += n;
            JSONObject root = new JSONObject(new String(buf, 0, off, "UTF-8"));
            JSONArray arr = root.optJSONArray("cidrs");
            if (arr == null) return out;
            for (int i = 0; i < arr.length(); i++) {
                JSONObject o = arr.optJSONObject(i);
                if (o == null) continue;
                Entry e = new Entry();
                e.cidr = o.optString("cidr", "");
                e.colo = o.optString("colo", "");
                e.area = o.optString("area", "");
                e.rttMs = (float) o.optDouble("rtt_ms", 0);
                e.ip = o.optString("ip", "");
                if (e.cidr.length() > 0) out.add(e);
            }
        } catch (Throwable t) {
            Log.w(TAG, "读段库失败: " + t.getMessage());
        } finally {
            try { if (in != null) in.close(); } catch (Throwable ignored) { }
        }
        return out;
    }

    static void save(Context ctx, String slot, List<Entry> list) {
        FileOutputStream os = null;
        try {
            JSONObject root = new JSONObject();
            root.put("ts", System.currentTimeMillis() / 1000);
            root.put("probe", CfIpPool.PROBE_HOST);
            root.put("count", list.size());
            JSONArray arr = new JSONArray();
            for (Entry e : list) {
                JSONObject o = new JSONObject();
                o.put("cidr", e.cidr);
                o.put("colo", e.colo == null ? "" : e.colo);
                o.put("area", e.area == null ? "" : e.area);
                o.put("rtt_ms", (int) e.rttMs);
                o.put("ip", e.ip == null ? "" : e.ip);
                arr.put(o);
            }
            root.put("cidrs", arr);
            os = new FileOutputStream(libFile(ctx, slot));
            os.write(root.toString().getBytes("UTF-8"));
            os.flush();
        } catch (Throwable t) {
            Log.w(TAG, "写段库失败: " + t.getMessage());
        } finally {
            try { if (os != null) os.close(); } catch (Throwable ignored) { }
        }
    }

    /**
     * 全量扫结束后重建段库。
     * 数据源是 COLO_CACHE —— 里面是这一轮真正问出过 colo 的 IP，
     * 也就是「哪些段落在哪个区域」的实证。每个 /24 只留一个代表。
     */
    public static int rebuildFromCache(Context ctx, String slot) {
        Map<String, Entry> byCidr = new HashMap<>();
        for (Map.Entry<String, String> kv : CfIpPool.COLO_CACHE.entrySet()) {
            String ip = kv.getKey();
            String colo = kv.getValue();
            if (TextUtils.isEmpty(ip) || TextUtils.isEmpty(colo)) continue;
            String cidr = cidr24(ip);
            if (byCidr.containsKey(cidr)) continue;
            Entry e = new Entry();
            e.cidr = cidr;
            e.colo = colo;
            String area = CfIpPool.COLO_AREA.get(colo);
            e.area = area == null ? "" : area;
            e.ip = ip;
            e.rttMs = 0f;
            byCidr.put(cidr, e);
        }
        List<Entry> list = new ArrayList<>(byCidr.values());
        if (list.isEmpty()) {
            Log.w(TAG, "COLO_CACHE 为空，段库未更新");
            return 0;
        }
        /* 目标区域排前面，方便人工核对；Stage1 会重新测延迟，这里的顺序只是参考 */
        Collections.sort(list, new Comparator<Entry>() {
            @Override public int compare(Entry a, Entry b) {
                boolean ta = CarrierDetect.isAsiaPac(a.area);
                boolean tb = CarrierDetect.isAsiaPac(b.area);
                if (ta != tb) return ta ? -1 : 1;
                return a.cidr.compareTo(b.cidr);
            }
        });
        save(ctx, slot, list);
        int ap = 0;
        for (Entry e : list) if (CarrierDetect.isAsiaPac(e.area)) ap++;
        Log.i(TAG, "段库已重建（" + slot + "）: " + list.size() + " 段，其中亚太 " + ap + " 段");
        return list.size();
    }

    /**
     * 两级优选。返回按延迟升序的 IP 列表（可能为空 = 段库不可用，调用方需回退）。
     *
     * @param carrierOut 长度 >=1 时，把识别出的运营商名写进 [0]，供界面展示
     */
    public static List<String> pickTop(Context ctx, String slot, int wantN,
                                       Network bindNet, String[] carrierOut) {
        List<String> result = new ArrayList<>();
        if (wantN <= 0) wantN = 5;

        String carrier = CarrierDetect.detect(ctx, slot);
        if (carrierOut != null && carrierOut.length > 0) carrierOut[0] = carrier;
        String regionsCsv = CarrierDetect.regionsFor(carrier);
        String fbCsv = CarrierDetect.fallbackFor(carrier);
        Set<String> areas = CarrierDetect.areaSet(regionsCsv);
        Set<String> fbAreas = CarrierDetect.areaSet(fbCsv);

        List<Entry> lib = load(ctx, slot);
        if (lib.isEmpty()) {
            Log.i(TAG, "段库不存在（" + slot + "），需要先跑一次全量扫");
            return result;
        }

        /* 分级：上次就落目标区域的段最可能仍有效；上次落其他亚太的也可能因 BGP 漂移变过来 */
        List<Entry> cand = new ArrayList<>();
        List<Entry> tierB = new ArrayList<>();
        for (Entry e : lib) {
            if (areas.contains(e.area)) cand.add(e);
            else if (CarrierDetect.isAsiaPac(e.area)) tierB.add(e);
        }
        cand.addAll(tierB);
        if (cand.isEmpty()) cand.addAll(lib);        /* 极端情况：整库都不在亚太，全拿来试 */
        if (cand.size() > STAGE1_MAX) {
            cand = new ArrayList<>(cand.subList(0, STAGE1_MAX));
        }

        long t0 = System.currentTimeMillis();
        List<Entry> segHits = probeSegments(cand, bindNet);
        long t1 = System.currentTimeMillis();

        List<Entry> primary = filterByArea(segHits, areas);
        boolean usedFallback = false;
        int n = Math.min(STAGE2_DEEP_CIDR, primary.size());
        int capacity = n * (STAGE2_PER_CIDR + 1);
        if (capacity < wantN && !fbAreas.isEmpty()) {
            Set<String> merged = new HashSet<>(areas);
            merged.addAll(fbAreas);
            List<Entry> alt = filterByArea(segHits, merged);
            if (alt.size() > primary.size()) {
                primary = alt;
                areas = merged;
                usedFallback = true;
            }
        }
        if (primary.isEmpty()) {
            Log.w(TAG, "段池：目标区域 0 段合格（复测 " + cand.size() + " 段）");
            return result;
        }

        List<Entry> deep = buildDeepCandidates(primary);
        List<Entry> hits = probeSegments(deep, bindNet);
        List<Entry> pool = filterByArea(hits, areas);
        if (pool.size() < wantN) {
            Set<String> seenIp = new HashSet<>();
            for (Entry e : pool) seenIp.add(e.ip);
            for (Entry e : primary) {
                if (pool.size() >= wantN) break;
                if (e.ip != null && seenIp.add(e.ip)) pool.add(e);
            }
        }
        Collections.sort(pool, new Comparator<Entry>() {
            @Override public int compare(Entry a, Entry b) {
                return Float.compare(a.rttMs, b.rttMs);
            }
        });
        Set<String> uniq = new HashSet<>();
        for (Entry e : pool) {
            if (result.size() >= wantN) break;
            if (e.ip != null && uniq.add(e.ip)) result.add(e.ip);
        }

        Log.i(TAG, "段池优选: " + carrier + " → " + regionsCsv
                + (usedFallback ? "（主区域不足已降级）" : "")
                + " | 段库 " + lib.size() + " 段，Stage1 " + cand.size() + " 段 "
                + (t1 - t0) + "ms → " + primary.size() + " 段合格，Stage2 " + deep.size()
                + " 个 → 产出 " + result.size() + " 个，共 "
                + (System.currentTimeMillis() - t0) + "ms");
        return result;
    }

    /* ---------------- 内部 ---------------- */

    private static List<Entry> filterByArea(List<Entry> src, Set<String> areas) {
        List<Entry> out = new ArrayList<>();
        if (src == null) return out;
        for (Entry e : src) {
            if (e.area != null && areas.contains(e.area)) out.add(e);
        }
        return out;
    }

    /** 并发探测一批 IP（每段 1 个或段内多个），返回命中结果 */
    private static List<Entry> probeSegments(List<Entry> list, final Network bindNet) {
        if (list == null || list.isEmpty()) return new ArrayList<>();
        final List<Entry> hits = Collections.synchronizedList(new ArrayList<Entry>());
        final int threads = Math.min(STAGE1_THREADS, Math.max(1, list.size()));
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        final CountDownLatch latch = new CountDownLatch(list.size());
        for (final Entry e : list) {
            pool.execute(new Runnable() {
                @Override public void run() {
                    try {
                        String ip = (!TextUtils.isEmpty(e.ip)) ? e.ip : (e.cidr + ".1");
                        CfIpPool.ColoProbe r = CfIpPool.probeColoEx(ip, bindNet);
                        if (r == null) {
                            /* 代表 IP 挂了不等于整段挂了，换个 host 再试一次 */
                            String alt = e.cidr + ".129";
                            r = CfIpPool.probeColoEx(alt, bindNet);
                            if (r != null) ip = alt;
                        }
                        if (r != null) {
                            Entry hit = new Entry();
                            hit.cidr = e.cidr;
                            hit.colo = r.colo;
                            hit.area = CfIpPool.COLO_AREA.get(r.colo);
                            hit.rttMs = r.latencyMs;
                            hit.ip = ip;
                            if (hit.area != null) hits.add(hit);
                        }
                    } catch (Throwable ignored) {
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        try { latch.await(90, TimeUnit.SECONDS); } catch (Throwable ignored) { }
        pool.shutdownNow();
        return new ArrayList<>(hits);
    }

    /** 取最快几段，每段抽若干 IP；代表 IP 也带上 */
    private static List<Entry> buildDeepCandidates(List<Entry> primary) {
        List<Entry> sorted = new ArrayList<>(primary);
        Collections.sort(sorted, new Comparator<Entry>() {
            @Override public int compare(Entry a, Entry b) {
                return Float.compare(a.rttMs, b.rttMs);
            }
        });
        int n = Math.min(STAGE2_DEEP_CIDR, sorted.size());
        Random rnd = new Random();
        List<Entry> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < n; i++) {
            Entry e = sorted.get(i);
            String rep = (!TextUtils.isEmpty(e.ip)) ? e.ip : (e.cidr + ".1");
            if (seen.add(rep)) {
                Entry c = new Entry();
                c.cidr = e.cidr; c.ip = rep; c.area = e.area; c.colo = e.colo; c.rttMs = e.rttMs;
                out.add(c);
            }
            for (int k = 0; k < STAGE2_PER_CIDR; k++) {
                String ip = e.cidr + "." + (rnd.nextInt(253) + 2);
                if (seen.add(ip)) {
                    Entry c = new Entry();
                    c.cidr = e.cidr; c.ip = ip;
                    out.add(c);
                }
            }
        }
        return out;
    }

    static String cidr24(String ip) {
        if (ip == null) return "";
        int i = ip.lastIndexOf('.');
        return i > 0 ? ip.substring(0, i) : ip;
    }
}
