# NVA2DLNA

NVA2DLNA 在局域网中模拟一台哔哩哔哩 NVA 电视接收器，并把收到的播放请求转发给你在网页中选定的标准 DLNA MediaRenderer。

它只做一件事：

~~~
哔哩哔哩 App --NVA--> NVA2DLNA --HTTP/DLNA SOAP--> 电视或播放器
                            |
                            +-- DASH 视频 + 音频 --FFmpeg copy-remux--> MPEG-TS
~~~

弹幕与倍速命令会正常确认后忽略，不会传给 DLNA 设备。Pause、Resume、Stop、Seek、音量和清晰度切换会尽可能映射到 DLNA。

## 功能

- 经过验证的 NVA E0/C0/E4 帧协议和 /projection 会话。
- 完整 NVA SSDP 身份，手机中显示为“我的小电视”。
- 极简网页只负责扫描并选择局域网 DLNA 播放器；选择结果按 UDN 保存。
- Progressive 媒体经带 Bilibili 请求头的本机代理转发。
- 高清 DASH 选择 H.264 视频与 AAC 音频，FFmpeg 无损合并为单路 MPEG-TS。
- 过滤自身设备，避免桥接回路。
- Rust 主程序不链接 FFmpeg 动态库，可原生构建 x86_64/arm64。
- Linux 多架构容器包含 FFmpeg；Windows 可使用 PATH 或指定 ffmpeg.exe。

## Windows 本机运行

要求：

- Rust 1.88 或更高版本
- Node.js 22.12 或更高版本
- FFmpeg（放入 PATH，或设置 NVA2DLNA_FFMPEG）

~~~powershell
cd web
npm ci
npm run build
cd ..
cargo run --release
~~~

启动后访问 http://本机局域网IP:8080，点击“重新扫描”并选择 DLNA 设备。随后在哔哩哔哩 App 中选择“我的小电视”。

若 Windows 防火墙询问，请允许专用网络访问。需要放行：

- TCP 8080：管理页与发送给 DLNA 的媒体
- TCP 9959：NVA 描述和控制会话
- UDP 1900：SSDP 发现

多网卡机器可显式指定用于投屏的 IPv4：

~~~powershell
target\release\nva2dlna.exe --advertise-ip 192.168.1.20
~~~

## Linux / Podman

容器必须使用 host 网络，否则 SSDP 组播及 DLNA 设备回连媒体地址通常无法工作。

~~~bash
podman compose up -d --build
~~~

多网卡或启用了 VPN 时，建议在 compose.yml 中设置 NVA2DLNA_ADVERTISE_IP 为 DLNA 设备能够访问的局域网 IPv4。

构建单个平台。若在 amd64 主机上构建 arm64（或反向构建），主机必须先配置对应的 binfmt/QEMU；也可以分别在两个原生架构主机上构建：

~~~bash
podman build --platform linux/amd64 -t nva2dlna:amd64 .
podman build --platform linux/arm64 -t nva2dlna:arm64 .
~~~

要生成多架构 manifest，可在已配置对应架构执行环境的构建机上运行：

~~~bash
podman manifest create nva2dlna:latest
podman build --platform linux/amd64 --manifest nva2dlna:latest .
podman build --platform linux/arm64 --manifest nva2dlna:latest .
~~~

## GitHub Actions 构建

`.github/workflows/build.yml` 会在 push、pull request 或手动触发时，分别使用原生 GitHub 托管环境构建并上传：

- `nva2dlna-windows-amd64.zip`
- `nva2dlna-linux-amd64.tar.gz`
- `nva2dlna-linux-arm64.tar.gz`

每个压缩包都包含原生可执行文件、`web/dist`、README 和许可证。请完整解压并从包的根目录启动程序；Linux 压缩包保留可执行权限。原生发布包不捆绑 FFmpeg，运行前仍需将对应平台的 `ffmpeg` 放入 PATH，或通过 `NVA2DLNA_FFMPEG` 指定路径。容器镜像则已经包含 FFmpeg。

只有推送 Git tag 时才会自动创建同名 GitHub Release：三个平台全部构建成功后，工作流会生成发行说明并上传上述三个压缩包。普通分支 push、pull request 和手动运行只生成 Actions artifact，不会发布 Release；同一 tag 的工作流重试会更新已有 Release 的附件。

## 配置

所有命令行参数都有对应环境变量：

| 参数 | 环境变量 | 默认值 |
|---|---|---|
| --web-listen | NVA2DLNA_WEB_LISTEN | 0.0.0.0:8080 |
| --nva-listen | NVA2DLNA_NVA_LISTEN | 0.0.0.0:9959 |
| --advertise-ip | NVA2DLNA_ADVERTISE_IP | 自动选择 |
| --config | NVA2DLNA_CONFIG | data/config.json |
| --web-dir | NVA2DLNA_WEB_DIR | web/dist |
| --ffmpeg | NVA2DLNA_FFMPEG | ffmpeg |
| --friendly-name | NVA2DLNA_NAME | 我的小电视 |

配置文件只保存稳定接收器 UUID 和选中的 DLNA UDN。Bilibili access key、签名媒体 URL 与临时媒体 token 仅保存在内存中，不写入网页或磁盘。

## 当前媒体策略

- Progressive MP4/音频：保留上游 Range/Content-Range，直接代理给目标设备。
- FLV/HLS：经带 Bilibili 请求头的安全递归代理，再由 FFmpeg `-c copy` 重封装为 MPEG-TS。
- WebM：保持原格式直接代理，由目标 DLNA 播放器决定是否支持，避免把 VP8/VP9/Opus 错误封装进 MPEG-TS。
- DASH：视频和音频先经 loopback 代理，再由 FFmpeg `-c copy` 合并为 MPEG-TS。
- 同一媒体允许两个有界 FFmpeg 消费者，以兼容 4K 播放器常见的探测连接与正式播放连接；更多连接会短暂等待后限流。
- 目标播放器必须支持 HTTP 流式 MPEG-TS。对只接受完整 MP4/Content-Length 的老旧 DMR，后续可增加“先缓存完整 MP4 再播放”的兼容模式。
- Seek 只对可定位的直连 Progressive 资源可靠；流式 DASH/FLV/HLS 转封装输出不声明 Seek 能力。
- 同一时刻只维护一个活动投屏；新 Play 会替换旧媒体 token。
- 播放中更换目标会先停止当前投屏，再保存新的默认目标。

## 开发与测试

~~~bash
cargo fmt --all -- --check
cargo test --all-targets
~~~

测试覆盖 NVA 分帧/粘包、SSDP 请求、Bilibili 高清选轨、URL 边界，以及假的 DLNA MediaRenderer 上 SetAVTransportURI -> Play -> Stop 调用顺序。
