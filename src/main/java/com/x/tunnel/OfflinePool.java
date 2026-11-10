package com.x.tunnel;

import android.content.Context;
import android.util.Log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * 离线兜底池 —— 历史长期优选留下的最佳 50 个 IP。
 *
 * <p>为什么要有它：段池每轮选出来的 Top IP 一直在小幅漂移（同段换 IP、第 5/6 名
 * 互换），而 Cloudflare 侧的网络状况本身也阶段性波动。真正想要的不是「此刻最快」，
 * 而是「长期都不差」。所以维护一个 50 个的离线池作为稳定退路：
 * <ul>
 *   <li>每次黄金优选完成后，把 Top1 追加进池（一行一个，最旧在上）；</li>
 *   <li>已经在池里的 IP 再次入选时，先摘掉原位置再追加到队尾（刷新新鲜度）；</li>
 *   <li>超过 50 个就从队头淘汰。</li>
 * </ul>
 *
 * <p>「重复入选就刷新到队尾」是关键：它让淘汰规则同时具备 FIFO 与 LRU 的性质 ——
 * 一直好用的 IP 会因为反复入选不断被推到队尾，只有加进来之后再也上不了榜的，
 * 才会被挤出去。单纯按加入时间 FIFO 会误杀仍然优质的 IP。
 *
 * <p>兜底使用时照常走初筛 + 精测，已失效的 IP 自然落榜，不会污染榜单。
 * 文件格式与 Windows 端 cf_ip_offline.txt 一致，方便两边互拷。
 */
public class OfflinePool {
    private static final String TAG = "OfflinePool";
    public static final int MAX = 50;

    private static File file(Context ctx) {
        return new File(ctx.getFilesDir(), "cf_ip_offline.txt");
    }

    /** 按顺序读出全部条目（最旧 → 最新）。 */
    public static synchronized List<String> load(Context ctx) {
        List<String> out = new ArrayList<>();
        File f = file(ctx);
        if (!f.exists()) return out;
        FileInputStream in = null;
        try {
            in = new FileInputStream(f);
            byte[] buf = new byte[(int) f.length()];
            int off = 0, n;
            while (off < buf.length && (n = in.read(buf, off, buf.length - off)) > 0) off += n;
            String text = new String(buf, 0, off, "UTF-8");
            for (String line : text.split("\n")) {
                String ip = line.trim();
                if (ip.length() >= 7 && ip.indexOf('.') > 0 && !out.contains(ip)) out.add(ip);
            }
        } catch (Throwable t) {
            Log.w(TAG, "读离线池失败: " + t.getMessage());
        } finally {
            try { if (in != null) in.close(); } catch (Throwable ignored) { }
        }
        return out;
    }

    public static synchronized int count(Context ctx) {
        return load(ctx).size();
    }

    /** 把本轮最佳 IP 推进池子。返回 true 表示是新面孔入池（不是单纯刷新位置）。 */
    public static synchronized boolean push(Context ctx, String ip) {
        if (ip == null || ip.isEmpty()) return false;
        List<String> list = load(ctx);
        boolean existed = list.remove(ip);          /* 命中则先摘掉，稍后追加到队尾 */
        list.add(ip);
        while (list.size() > MAX) list.remove(0);   /* 超限从队头（最旧）淘汰 */
        save(ctx, list);
        return !existed;
    }

    private static void save(Context ctx, List<String> list) {
        FileOutputStream os = null;
        try {
            os = new FileOutputStream(file(ctx));
            StringBuilder sb = new StringBuilder();
            for (String ip : list) sb.append(ip).append('\n');
            os.write(sb.toString().getBytes("UTF-8"));
            os.flush();
        } catch (Throwable t) {
            Log.w(TAG, "写离线池失败: " + t.getMessage());
        } finally {
            try { if (os != null) os.close(); } catch (Throwable ignored) { }
        }
    }
}
