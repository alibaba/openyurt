# openyurtio/openyurt

<div align="center">

<img src="docs/img/OpenYurt.png" width="400" height="94"><br/>

[![Version](https://img.shields.io/badge/OpenYurt-v0.4.0-orange)](CHANGELOG.md)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Go Report Card](https://goreportcard.com/badge/github.com/openyurtio/openyurt)](https://goreportcard.com/report/github.com/openyurtio/openyurt)

</div>

[English](./README.md) | 简体中文

|![notification](docs/img/bell-outline-badge.svg) What is NEW!|
|------------------|
| 2021-05-21  OpenYurt v0.4.0  **正式发布**! 请查看 [CHANGELOG](CHANGELOG.md) 来获得更多更新细节.|
| 2021-01-08  OpenYurt v0.3.0  **正式发布**! 请查看 [CHANGELOG](CHANGELOG.md) 来获得更多更新细节.|
| 2020-08-30  OpenYurt v0.2.0  **正式发布**! 请查看 [CHANGELOG](CHANGELOG.md) 来获得更多更新细节.|
| 2020-05-29  OpenYurt v0.1.0-beta.1  **正式发布**! 请查看 [CHANGELOG](CHANGELOG.md) 来获得更多更新细节.|

OpenYurt (官网: <https://openyurt.io>) 是托管在 Cloud Native Computing Foundation (CNCF) 下的 [沙箱项目](https://www.cncf.io/sandbox-projects/). 它是基于原生 Kubernetes 构建的，目标是扩展 Kubernetes 以无缝支持边缘计算场景。简而言之，OpenYurt 使客户可以像在公共云基础设施上运行应用一样管理在边缘基础设施之上运行的应用。

OpenYurt 适合如下这些常见的边缘计算用户场景：
- 使设备和负载之间的长途通信网络通讯流量最小化；
- 克服网络带宽限制及可靠性限制；
- 在边缘节点处理数据以减少延迟；
- 为敏感数据的处理提供了一个更好的安全模型；
- 在单个集群中管理分散在不同地域的边缘资源和边缘应用；

就兼容性和可用性而言，OpenYurt 具有以下优点：
- **Kubernetes 原生**。它提供了完整的 Kubernetes API 兼容性；支持所有 Kubernetes 工作负载，服务，运营商，CNI 插件和 CSI 插件。
- **无缝转换**。它提供了一种工具，可以轻松地将本地 Kubernetes 转换为“边缘就绪”的集群；同时 OpenYurt 组件的额外资源和维护成本非常低。
- **节点自治**。它提供了容忍不稳定或断开连接的云边缘网络的机制。即使边缘节点脱机，在边缘节点中运行的应用程序也不会受到影响。
- **与云平台无关**。 OpenYurt 可以轻松部署在任何公共云 Kubernetes 服务中。

## 架构

OpenYurt 遵循经典的边缘应用程序架构设计 ：Kubernetes 集群 的 master 节点集中部署于公共云中，由这些 master 节点管理位于边缘站点的多个边缘节点。每个边缘节点具有适度的计算资源，从而允许运行大量边缘应用以及 Kubernetes 节点守护进程。集群中的边缘节点可以分处于在多个物理区域中（region）。在OpenYurt 的概念中  区域（region）这个概念 和 池（Pool）这个概念 是可以相互转换的。
<div align="left">
  <img src="docs/img/arch.png" width=70% title="OpenYurt architecture">
</div>

OpenYurt 的主要组件包括：
- **YurtHub**：Kubernetes 集群中节点上运行的守护程序，它的作用是作为（Kubelet，Kubeproxy，CNI 插件等）的出站流量的代理。它在边缘节点的本地存储中缓存     Kubernetes 节点守护进程可能访问的所有资源的状态。如果边缘节点离线，则这些守护程序可以帮助节点在重新启动后恢复状态。
- **Yurt Controller Manager**：在各种不同的边缘计算用例中 Yurt Controller Manager 负责管理一个节点控制器（ Node Controller ）。举例来说即使节点心跳丢失，处于自治模式的节点中的Pod也不会从 API Server 中被驱逐（ evicted ）。
- **Yurt App Manager**：它管理OpenYurt中引入的两个CRD资源。*[NodePool](docs/enhancements/20201211-nodepool_uniteddeployment.md)* 和 *[UnitedDeployment](docs/enhancements/20201211-nodepool_uniteddeployment.md)*. 前者为位于同一区域的节点池提供了便利的管理方法。
后者定义了一种新的边缘应用模型以节点池为单位来管理工作负载。
- **Yurt Tunnel (server/agent)**：`TunnelServer`通过反向代理与在每个边缘节点中运行的 TunnelAgent 守护进程建立连接并以此在公共云的控制平面 与 处于 企业内网（ Intranet ）环境的边缘节点之间建立安全的网络访问。

## 开始之前

[资源和系统要求](./docs/resource-and-system-requirements-cn.md)

## 开始使用
OpenYurt 支持最高版本为1.16的 Kubernetes 。使用更高版本的 Kubernetes 可能会导致兼容性问题。
您可以[手动](docs/tutorial/manually-setup.md)设置 OpenYurt 集群，但是我们建议使用 `yurtctl` 命令行工具启动 OpenYurt 。要快速构建和安装设置 `yurtctl` ，在编译系统已安装了 golang 1.13+ 和 bash 的前提下你可以执行以下命令来完成安装：

```bash
git clone https://github.com/openyurtio/openyurt.git
cd openyurt
make WHAT=cmd/yurtctl
```

`yurtctl` 的二进制文件位于_output /bin 目录。如果需要将已存在的 Kubernetes 集群转换为 OpenYurt 集群，你可以使用如下简单的命令(目前支持minikube, kubeadm, ack, kind 四种工具安装的kubernetes集群)：

```bash
_output/bin/yurtctl convert --provider [minikube|kubeadm|ack|kind]
```

要卸载 OpenYurt 并恢复为原始的 Kubernetes 集群设置，请运行以下命令：

```bash
_output/bin/yurtctl revert
```

请查看 [yurtctl教程](./docs/tutorial/yurtctl.md)来获得更多使用细节。

## 使用方法
我们提供详细的[教程](./docs/tutorial/README.md)来演示如何使用 OpenYurt 管理部署在边缘节点上的应用。

## 发展规划
[2021年 发展规划](docs/roadmap.md)

## 社区
### 贡献
如果您愿意为 OpenYurt 项目做贡献，请参阅我们的 [CONTRIBUTING](CONTRIBUTING.md) 文档以获取详细信息。我们还准备了开发人员指南来帮助代码贡献者。

### 周会

| Item        | Value  |
|---------------------|---|
| 社区会议 | [从2020.9.2开始双周会议，周三上午11:00～12：00(北京时间)](https://calendar.google.com/calendar/u/0?cid=c3VudDRtODc2Y2c3Ymk3anN0ZDdkbHViZzRAZ3JvdXAuY2FsZW5kYXIuZ29vZ2xlLmNvbQ) |
| 会议链接 | https://us02web.zoom.us/j/82828315928?pwd=SVVxek01T2Z0SVYraktCcDV4RmZlUT09 |
| 会议纪要| [会议议程及纪要](https://shimo.im/docs/rGK3cXYWYkPrvWp8) |
| 会议视频| [B站 OpenYurt](https://space.bilibili.com/484245424/video) |

### 联络方式
如果您对本项目有任何疑问或想做出贡献，欢迎通过 github issue 或 pull request 来沟通相关问题，其他有效的沟通渠道如下所示：

- 邮件组: https://groups.google.com/g/openyurt/
- Slack: [channel](https://join.slack.com/t/openyurt/shared_invite/zt-rc5ecz4h-sEWU1vYx5gzc3_zx3En0jg)
- Dingtalk Group (钉钉讨论群)

<div align="left">
  <img src="docs/img/ding.jpg" width=25% title="dingtalk">
</div>

## 许可证
OpenYurt 遵循 Apache 2.0许可证。有关详细信息请参见 [LICENSE](LICENSE) 文件。 OpenYurt 中的某些特定实现是基于 Kubernetes 的现有代码，这些实现都应归功于Kubernetes相关代码的原作者。
