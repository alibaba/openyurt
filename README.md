# openyurtio/openyurt

<div align="center">

<img src="docs/img/OpenYurt.png" width="400" height="94"><br/>

[![Version](https://img.shields.io/badge/OpenYurt-v0.4.0-orange)](CHANGELOG.md)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Go Report Card](https://goreportcard.com/badge/github.com/openyurtio/openyurt)](https://goreportcard.com/report/github.com/openyurtio/openyurt)

</div>

English | [简体中文](./README.zh.md)

|![notification](docs/img/bell-outline-badge.svg) What is NEW!|
|------------------|
|March 21th, 2021. OpenYurt v0.4.0 is **RELEASED**! Please check the [CHANGELOG](CHANGELOG.md) for details.|
|January 8th, 2021. OpenYurt v0.3.0 is **RELEASED**! Please check the [CHANGELOG](CHANGELOG.md) for details.|
|August 30th, 2020. OpenYurt v0.2.0 is **RELEASED**! Please check the [CHANGELOG](CHANGELOG.md) for details.|
|May 29th, 2020. OpenYurt v0.1.0-beta.1 is **RELEASED**! Please check the [CHANGELOG](CHANGELOG.md) for details.|

OpenYurt(official website: <https://openyurt.io>) is now hosted by the Cloud Native Computing Foundation(CNCF) as a [Sandbox Level Project](https://www.cncf.io/sandbox-projects/). It is built based on native Kubernetes and targets to extend it to support edge computing seamlessly.
In a nutshell, OpenYurt enables users to manage applications that run in the edge infrastructure as if they were running
in the cloud infrastructure.

OpenYurt is suitable for common edge computing use cases whose requirements include:
- Minimizing the network traffic over long distances between the devices and the workloads.
- Overcoming the network bandwidth or reliability limitations.
- Processing data remotely to reduce latency.
- Providing a better security model to handle sensitive data.
- Manage edge resources and edge applications in a single cluster.

OpenYurt has the following advantages in terms of compatibility and usability.
- **Kubernetes native**. It provides full Kubernetes API compatibility. All Kubernetes workloads, services,
  operators, CNI plugins, and CSI plugins are supported.
- **Seamless conversion**. It provides a tool to easily convert a native Kubernetes to be "edge" ready.
  The extra resource and maintenance costs of the OpenYurt components are very low.
- **Node autonomy**. It provides mechanisms to tolerate unstable or disconnected cloud-edge networking.
  The applications run in the edge nodes are not affected even if the nodes are offline.
- **Cloud platform agnostic**. OpenYurt can be easily deployed in any public cloud Kubernetes services.

## Architecture

OpenYurt follows a classic edge application architecture design -
a centralized Kubernetes master resides in the cloud site, which
manages multiple edge nodes reside in the edge site. Each edge node has moderate compute resources allowing
running a number of edge applications plus the Kubernetes node daemons. The edge nodes in a cluster can span
multiple physical regions. The terms `region` and `Pool` are interchangeable in OpenYurt.
<div align="left">
  <img src="docs/img/arch.png" width=70% title="OpenYurt architecture">
</div>

\
The major OpenYurt components consist of:
- **YurtHub**: A node daemon that serves as a proxy for the outbound traffic from the
  Kubernetes node daemons (Kubelet, Kubeproxy, CNI plugins and so on). It caches the
  states of all the resources that the Kubernetes node daemons
  might access in the edge node's local storage. In case the edge node is offline, those daemons can
  recover the states upon node restarts.
- **Yurt controller manager**: It manages a node controller for different edge computing use cases. For example,
  the Pods in the nodes that are in the `autonomy` mode will not be evicted from APIServer even if the
  node heartbeats are missing.
- **Yurt app manager**: It manages two CRD resources introduced in OpenYurt: *[NodePool](docs/enhancements/20201211-nodepool_uniteddeployment.md)*
  and *[UnitedDeployment](docs/enhancements/20201211-nodepool_uniteddeployment.md)*. The former provides a convenient
  management experience for a pool of nodes within the same region or site. The latter defines a new edge application management
  methodology of using per node pool workload.
- **Yurt tunnel (server/agent)**: `TunnelServer` connects with the `TunnelAgent` daemon running in each edge node via a
  reverse proxy to establish a secure network access between the cloud site control plane and the edge nodes
  that are connected to the intranet.
- **Node resource manager**: It manages local node resources of OpenYurt cluster in a unified manner.
  It currently manages LVM, QuotaPath and Pmem Memory.
  Please refer to [node-resource-manager](https://github.com/openyurtio/node-resource-manager) for more details.

## Before you begin

[Resource and system requirements](./docs/resource-and-system-requirements.md)

## Getting started

OpenYurt supports Kubernetes versions up to 1.16. Using higher Kubernetes versions may cause
compatibility issues.

You can setup the OpenYurt cluster [manually](docs/tutorial/manually-setup.md), but we recommend to start
OpenYurt by using the `yurtctl` command line tool. To quickly build and install `yurtctl`,
assuming the build system has golang 1.13+ and bash installed, you can simply do the following:

```bash
git clone https://github.com/openyurtio/openyurt.git
cd openyurt
make WHAT=cmd/yurtctl
```

The `yurtctl` binary can be found at `_output/bin`. To convert an existing Kubernetes cluster to an OpenYurt cluster,
the following simple command line can be used(support kubernetes clusters that managed by minikube, kubeadm, ACK and kind):

```bash
_output/bin/yurtctl convert --provider [minikube|kubeadm|ack|kind]
```

To uninstall OpenYurt and revert back to the original Kubernetes cluster settings, you can run the following command:

```bash
_output/bin/yurtctl revert
```

Please check [yurtctl tutorial](./docs/tutorial/yurtctl.md) for more details.

## Usage

We provider detailed [**tutorials**](./docs/tutorial/README.md) to demonstrate how to use OpenYurt to manage edge applications.

## Roadmap

- [2021 roadmap](docs/roadmap.md)

## Community

### Contributing

If you are willing to be a contributor for OpenYurt project, please refer to our [CONTRIBUTING](CONTRIBUTING.md) document for details.
We have also prepared a developer [guide](./docs/developer-guide.md) to help the code contributors.

### Meeting

| Item        | Value  |
|---------------------|---|
| APAC Friendly Community meeting | [Bi-weekly APAC (Starting Sep 2, 2020), Wednesday 11:00AM GMT+8](https://calendar.google.com/calendar/u/0?cid=c3VudDRtODc2Y2c3Ymk3anN0ZDdkbHViZzRAZ3JvdXAuY2FsZW5kYXIuZ29vZ2xlLmNvbQ) |
| Meeting link APAC Friendly meeting | https://us02web.zoom.us/j/82828315928?pwd=SVVxek01T2Z0SVYraktCcDV4RmZlUT09 |
| Meeting notes| [Notes and agenda](https://shimo.im/docs/rGK3cXYWYkPrvWp8) |
| Meeting recordings| [OpenYurt bilibili Channel](https://space.bilibili.com/484245424/video) |

### Contact

If you have any questions or want to contribute, you are welcome to communicate most things via GitHub issues or pull requests.
Other active communication channels:

- Mailing List: https://groups.google.com/g/openyurt/
- Slack: [channel](https://join.slack.com/t/openyurt/shared_invite/zt-rc5ecz4h-sEWU1vYx5gzc3_zx3En0jg)
- Dingtalk Group (钉钉讨论群)

<div align="left">
  <img src="docs/img/ding.jpg" width=25% title="dingtalk">
</div>

## License

OpenYurt is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.
Certain implementations in OpenYurt rely on the existing code from Kubernetes and the credits go to the original Kubernetes authors.
