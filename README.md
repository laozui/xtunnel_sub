# X-Tunnel Android with Subscription Support

基于 hev-socks5-tunnel + x-tunnel 内核开发的 Android 客户端，新增 INI 订阅解析与自动同步功能。

## 新增功能
- **订阅管理**：支持从远程 URL 获取订阅配置（支持明文 INI 格式与 Base64 编码）。
- **字段智能提取**：自动提取节点名称 (`name=`)、服务器地址 (`server=`)、身份令牌 (`token=`)、优选 IP (`ip=`)。
- **订阅同步机制**：支持手动同步、启动 App 自动同步、定时同步，更新时智能刷新订阅节点并完整保留用户手动添加的自定义节点。

## 构建方式
通过 GitHub Actions 自动完成 gomobile AAR 编译、C/NDK 编译与 APK 构建打包。
每次向 `main` 分支 push 代码即可自动触发并在 Actions Artifacts 下载最新的 Release APK。
