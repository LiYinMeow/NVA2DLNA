# NVA2DLNA

NVA2DLNA 在局域网中模拟一台哔哩哔哩 NVA 电视接收器，并把收到的播放请求转发给你在网页中选定的标准 DLNA MediaRenderer。

它只做一件事：

~~~
哔哩哔哩 App --NVA--> NVA2DLNA --HTTP/DLNA SOAP--> 电视或播放器
                            |
                            +-- DASH 视频 + 音频 --FFmpeg copy-remux--> MPEG-TS
~~~

弹幕开关会维护并向手机回播状态；弹幕正文会正常确认后忽略，因为标准 DLNA 没有弹幕通道。Pause、Resume、Stop、Seek、音量和清晰度切换会尽可能映射到 DLNA。

倍速（SwitchSpeed / PlaySpeed）按 AVTransport 的约定以 Play(Speed=) 转发给目标播放器。目标能不能吃分数倍速是它自己的属性，不是本机的：扫描设备时顺带读取它 AVTransport 描述里 TransportPlaySpeed 的 allowedValueList，只公告整数快进的电视遇上 1.5 时，NVA 面会跳过这一步（不报错，随后播报目标真正在跑的速率，正在播放的会话不受影响）；没有公告列表的目标按可以处理，真被播放器拒绝时仍照实返回 UPnP 错误。手机里的倍速菜单不是内置的，它来自设备播报的 SpeedChanged{supportSpeedList:[0.5,0.75,1.0,1.25,1.5,2.0]}：本机在投放成功、倍速变更、断线恢复三个时机重发这条播报，播报里的 currSpeed 是目标真正接受后的速率而不是手机请求的速率，所以被跳过时手机会自己跳回真正在播的那一档。直播没有可加速的时间轴，收到 SwitchSpeed 只确认不转发；清晰度切换会重投媒体，重投后按播报把当前倍速补回去。

4K：设备描述使用当前 UDashboard 的弹幕 + 4K 兼容值 255，并且 Android TV DASH 档位的 playurl 请求固定按 qn=120 探测——B 站接口只在请求本身打到该上限时才把 4K 写进 accept_quality。探测只让手机显示 4K 入口，实际播放仍按手机当前选择的清晰度取轨，不会静默升档。

另一条输入通道是乐播（LeLink/hpplay）原生 V1 接收端：UDP 25353 应答手机的 PTBL 探测，TCP 52288 受理发送端的文本 HTTP 指令，把乐播投屏转成 DLNA。

~~~
乐播 App --PTBL/LBTP + /play--> NVA2DLNA --HTTP/DLNA SOAP--> 电视或播放器
~~~

V1 没有加密也没有会话协商，指令集是 /server-info、/play、/send_videoInfo、/rate、/scrub、/add_volume、/sub_volume、/stop、/feedback。注意 /rate 的两个取值是暂停与恢复（0.000000 / 1.000000），不是倍速；乐播的真实倍速只存在于 V2 透传层，因此 V1 收到的投屏不改变目标播放速率。Seek 与进度都按秒换算。

## 功能

- 经过验证的 NVA E0/C0/E4 帧协议和 /projection 会话。
- 完整的七项 NVA SSDP 身份：发现 ST 使用 UDashboard/旧版验证过的 `schemas-upnp-org`，`app-bilibili-com` 仅作为描述 XML 的服务类型。线缆上的 `friendlyName` 固定为协议识别所需的“我的小电视”，可配置的 UniNVA 作为品牌名显示。
- 不发布普通 DLNA 输入设备；DLNA 只作为网页中可选择的播放输出，避免普通 MediaRenderer 响应遮蔽 NVA 身份。
- 乐播原生 V1 接收端：UDP 25353 的 PTBL/LBTP 探测应答与 TCP 52288 的文本 HTTP 指令面，设备名“UniLe”，投过来同样走本机媒体代理后转 DLNA。
- 两个投屏入口各自独立命名（UniNVA / UniLe），可分别用环境变量改写。
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

启动后访问 http://本机局域网IP:8080，点击“重新扫描”并选择 DLNA 设备。随后在哔哩哔哩 App 中选择带有“UniNVA”品牌后缀的小电视设备。

乐播原生投屏也不需要开关：手机上的乐播 App 会广播探测包，本机以“UniLe”出现在它的设备列表里，选择后即可投放。若手机搜不到，先确认手机与本机在同一二层网段（AP 隔离会吃掉广播），再检查下面两个端口。

若 Windows 防火墙询问，请允许专用网络访问。需要放行：

- TCP 8080：管理页和发送给 DLNA 播放器的媒体代理
- TCP 9958：NVA 描述和控制会话
- TCP 52288：乐播原生 V1 指令面
- UDP 1900：SSDP 发现
- UDP 25353：乐播原生 V1 探测应答

多网卡机器可显式指定用于投屏的 IPv4：

~~~powershell
target\release\nva2dlna.exe --advertise-ip 192.168.1.20
~~~

## Linux / Podman

容器必须使用 host 网络，否则 SSDP 组播及 DLNA 设备回连媒体地址通常无法工作。

~~~bash
podman compose up -d --build
~~~

也可以直接使用 GitHub Actions 发布的双架构镜像；Docker/Podman 会从同一个 manifest 自动选择 `linux/amd64` 或 `linux/arm64`：

~~~bash
podman pull ghcr.io/liyinmeow/nva2dlna:latest
podman compose pull
podman compose up -d --no-build
~~~

Compose 默认令 Web HTTP 服务监听所有 IPv4 网卡的 8080 端口。通过环境变量可以独立选择端口和本机网卡地址：

~~~bash
NVA2DLNA_WEB_IP=192.168.1.20 NVA2DLNA_WEB_PORT=18080 podman compose up -d
~~~

`NVA2DLNA_WEB_IP` 不设置、设为空值或设为 `*` 都表示监听全部网卡。因为管理页、API 和供电视读取的媒体代理共用这个 HTTP 监听器，指定单个 IP 也会把媒体回连与可选的目标扫描网卡限制到该地址；跨网卡网关模式应保持 `NVA2DLNA_WEB_IP=*`。容器使用 host 网络，因此无需也不应添加端口映射；`Containerfile` 中的 `EXPOSE 8080` 只是默认端口元数据，不限制自定义端口。

多网卡或启用了 VPN 时，`NVA2DLNA_ADVERTISE_IP` 只决定手机发现 UniNVA/UniLe 的接收侧地址；目标侧扫描网卡在管理页面中单独选择。

### 多网卡网关

管理页面可以多选用于搜索播放目标的网卡。程序会在每个所选 IPv4 网卡上分别发送 SSDP 和 LeLink mDNS 查询；同一台电视同时广播 DLNA 与乐播时仍合并成一个 LeLink 目标。投屏控制连接与发给播放器的媒体 URL 都固定走发现该目标的网卡，因此主机可以一侧接收手机投屏、另一侧连接电视网络。

- `--web-listen` 必须保持 `0.0.0.0:8080`（或其他通配地址/端口），否则目标侧网卡无法回连媒体代理；管理 API 会拒绝与单地址监听冲突的选择。
- Podman 必须使用 host network，bridge 网络无法可靠收发两侧的组播，也无法暴露主机各网卡地址。
- 这不是 mDNS/SSDP 路由器：只扫描主机直连的二层网络，或操作系统已有路由可达的网络；不会把任一侧的组播原样转发到另一侧。
- 留空配置表示自动扫描所有可用 LAN 网卡；页面选择按操作系统网卡名持久化，DHCP 换地址无需重新配置。

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

`.github/workflows/container.yml` 使用 Buildx/QEMU 对 `Containerfile` 同时构建 `linux/amd64` 与 `linux/arm64`，生成 Docker 与 Podman 都可使用的多架构 OCI 镜像：

- pull request 与手动运行只验证两个架构都能构建，不登录仓库，也不发布镜像；
- 推送 `master` 发布 `ghcr.io/liyinmeow/nva2dlna:edge` 和提交 SHA 标签；
- 推送 `v1.2.3` 这类稳定 SemVer tag 发布 `1.2.3`、`1.2`、`1` 与 `latest`；预发布 tag 不更新 `latest`。

GHCR 发布使用仓库自带的 `GITHUB_TOKEN`，无需额外配置 PAT。若仓库以前已创建过同名但未关联的 GHCR package，需要在 package 的 Actions access 中为本仓库授予写权限。GHCR package 首次发布时默认是私有的；若要直接使用上面的未认证拉取命令，应在 package 设置中把可见性改为 Public，否则先用具有 `read:packages` 权限的账号执行 `podman login ghcr.io` 或 `docker login ghcr.io`。

## 配置

所有命令行参数都有对应环境变量：

| 参数 | 环境变量 | 默认值 |
|---|---|---|
| --web-listen | NVA2DLNA_WEB_LISTEN | 0.0.0.0:8080 |
| --web-ip | NVA2DLNA_WEB_IP | 继承 --web-listen 的 IP；`*`/空值表示 0.0.0.0 |
| --web-port | NVA2DLNA_WEB_PORT | 继承 --web-listen 的端口 |
| --nva-listen | NVA2DLNA_NVA_LISTEN | 0.0.0.0:9958 |
| --lelink-listen | NVA2DLNA_LELINK_LISTEN | 0.0.0.0:52288 |
| --advertise-ip | NVA2DLNA_ADVERTISE_IP | 自动选择 |
| --config | NVA2DLNA_CONFIG | data/config.json |
| --web-dir | NVA2DLNA_WEB_DIR | web/dist |
| --ffmpeg | NVA2DLNA_FFMPEG | ffmpeg |
| --nva-name（兼容 --friendly-name） | NVA2DLNA_NVA_NAME（兼容 NVA2DLNA_NAME） | UniNVA |
| --dlna-name | NVA2DLNA_DLNA_NAME | UniDLNA（旧配置兼容，当前不发布 DLNA 输入设备） |
| --lelink-name | NVA2DLNA_LELINK_NAME | UniLe |

`--web-listen` / `NVA2DLNA_WEB_LISTEN` 继续兼容原有的 `IP:PORT` 写法；新的 `--web-ip` / `NVA2DLNA_WEB_IP` 和 `--web-port` / `NVA2DLNA_WEB_PORT` 若存在，会分别覆盖其中的 IP 与端口。新的独立端口参数必须为 1–65535。当前 HTTP 监听器同时承载 WebUI、管理 API 与媒体代理。

配置文件保存共享设备 UUID、独立的 NVA 设备 UUID、NVA 身份版本、选中的播放目标 UDN 和目标扫描网卡名。旧配置或识别指纹升级时只轮换一次 NVA UUID，并在 SSDP 上注销旧 NVA 身份，以清除发送端把旧 UDN 缓存为普通 DLNA 的错误分类；LeLink 身份和已选播放目标不会改变。Bilibili access key、签名媒体 URL 与临时媒体 token 仅保存在内存中，不写入网页或磁盘。

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
- 音量与静音会转发给真正的播放器；不支持 SetNextAVTransportURI，按 UPnP 返回 401 而不是伪报成功。
- 出站目标的倍速能力是扫描时多读一次它的 AVTransport 描述拿到的（2 秒超时、拿不到就当作未知），因此只在重新扫描后更新；恢复播放不受这张表限制，以免一个暂停过的会话被这张表永久留在暂停态。
- 乐播 V1 只给直链、起始位置和按秒的 Seek/进度，不给标题与时长：标题取 URL 末段（无则“乐播投屏”），进度实时向目标播放器查询后按秒回报。V1 的 /add_volume 与 /sub_volume 是相对量，本机自己记住当前音量（起始 50，每次 ±5）。
- 乐播 V1 没有倍速载体，收到 /rate 只会暂停或恢复；真实倍速需要 V2 透传层的 playRate，本机已留出到 DLNA 的映射位置但未实现 V2 握手。

## 开发与测试

~~~bash
cargo fmt --all -- --check
cargo test --all-targets
~~~

测试覆盖 NVA 分帧/粘包、旧版/UDashboard 的七项 NVA SSDP 身份、Bilibili 高清选轨、URL 边界，以及假的 DLNA MediaRenderer 上 SetAVTransportURI -> Play -> Stop 调用顺序；出站目标的能力位在这里验证为真机字节序：扫描时从目标的描述文件跟到它的 AVTransport 描述，读出 TransportPlaySpeed 的公告列表，列表里没有 1.5 就不投 1.5，而拒绝提供描述文件的目标仍然是可用目标。乐播原生面在 `tests/lelink_sink.rs` 里按发送端的真实字节序验证：UDP 探测的第三行 JSON、/play 的 text/parameters 正文、按秒的 Start-Position 与 /scrub、/rate 的暂停恢复、相对音量、进度回包的 `duration:`/`position:` 两行，以及重试的 /play 不会二次投放。`dmr` 模块的协议测试暂时保留，但运行时不再发布或挂载该输入端。
