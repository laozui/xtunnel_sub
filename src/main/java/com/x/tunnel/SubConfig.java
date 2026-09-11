package com.x.tunnel;

public class SubConfig {
    public static final String SUB_URL = "SubUrl";
    public static final String SUB_SYNC_INTERVAL = "SubSyncInterval";
    public static final String SUB_LAST_SYNC_TIME = "SubLastSyncTime";
    public static final String PROFILE_IS_SUB_PREFIX = "ProfileIsSub_";

    public static final int INTERVAL_MANUAL = 0; // 手动同步
    public static final int INTERVAL_ON_LAUNCH = 1; // 启动时
    public static final int INTERVAL_6_HOURS = 2; // 每6小时
    public static final int INTERVAL_DAILY = 3; // 每天
}
