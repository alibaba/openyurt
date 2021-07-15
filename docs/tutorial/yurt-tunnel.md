# Yurt-tunnel tutorial

## Use Yurt-tunnel to connect apiserver and edge node

In this tutorial, we will show how the yurt-tunnel helps the apiserver send
request to nodes when the network traffic from apiserver to the node is
blocked. To mimic the real scenario where the cloud node and edge nodes may
locate in separate network regions, we use a two-nodes minikube as the
experimental cluster.

### 1. Provision a minikube cluster

Start from version 1.10, minikube allows users to provision multinode clusters.
Depending on the version you are using, minikube may use docker as the default
driver, which is not supported by yurt-tunnel. Therefore, make sure to choose
hyperkit or virtualbox as your driver. For example, the OSX user can create a
two-nodes minikube cluster by typing the following command:

```bash
minikube start --nodes 2 --driver hyperkit
```

If everything goes right, we will have a two-nodes cluster up and running:

```bash
$ kubectl get nodes -o wide
NAME           STATUS   ROLES    AGE     VERSION   INTERNAL-IP    EXTERNAL-IP   OS-IMAGE               KERNEL-VERSION   CONTAINER-RUNTIME
minikube       Ready    master   3h50m   v1.18.3   192.168.64.3   <none>        Buildroot 2019.02.11   4.19.114         docker://19.3.12
minikube-m02   Ready    <none>   3h48m   v1.18.3   192.168.64.9   <none>        Buildroot 2019.02.11   4.19.114         docker://19.3.12
```

In the rest of this tutorial, we will assume that the node named `minikube` is the
cloud node, and the node named `minikube-m02` is the edge node.

### 2. Create a test pod

As we plan to test the functionality of routing requests from the apiserver to
nodes, which usually happens when the apiserver receives requests of accessing
the pods, let's create a test pod that will run on the edge node.

```bash
$ kubectl apply -f-<<EOF
apiVersion: v1
kind: Pod
metadata:
  name: test-po
  namespace: default
spec:
  nodeName: minikube-m02
  containers:
  - name: test-po
    image: busybox
    command:
    - top
EOF
```
After the `test-po` is running, we can check the connection between the apiserver
and the node by typing the following commands:

```bash
$ kubectl exec test-po -- date
Fri Aug  7 23:17:27 UTC 2020
```

### 4. Block the network traffic from the apiserver to the node

Next, let's block the network traffic from the apiserver to the node by dropping
network packages that are sent from the apiserver to the node. Specifically,
we add a nat rule to the cloud node that drops all packets to the node with
destination port set as 10250 (kubelet listens on port 10250 which receives
https request from the apiserver).

```bash
$ minikube ssh
                         _             _
            _         _ ( )           ( )
  ___ ___  (_)  ___  (_)| |/')  _   _ | |_      __
/' _ ` _ `\| |/' _ `\| || , <  ( ) ( )| '_`\  /'__`\
| ( ) ( ) || || ( ) || || |\`\ | (_) || |_) )(  ___/
(_) (_) (_)(_)(_) (_)(_)(_) (_)`\___/'(_,__/'`\____)

$ sudo iptables -A OUTPUT -p tcp -d 192.168.64.9 --dport 10250 -j DROP
```

Now, if we try to execute the `date` command in `test-po` again, the command
will hang.

### 5. Setup the yurt-tunnel manually

It is recommended to use `yurtctl` tool to deploy yurt-tunnel components by
adding the `--deploy-yurttunnel` option when coverting a Kubernetes cluster. For example,
```bash
yurtctl convert --cloud-nodes minikube --provider minikube --deploy-yurttunnel
```

You may also setup the yurt-tunnel manually by deploying yurt-tunnel-server
and yurt-tunnel-agent separately.

To set up the yurt-tunnel-server, let's first add a label to the cloud node
```bash
kubectl label nodes minikube openyurt.io/is-edge-worker=false
```

Then, we can deploy the yurt-tunnel-server:
```bash
$ kubectl apply -f config/setup/yurt-tunnel-server.yaml
```

Next, we can set up the yurt-tunnel-agent. Like before, we add a label to the
edge node, which allows the yurt-tunnel-agent to be run on the edge node:

```bash
kubectl label nodes minikube-m02 openyurt.io/is-edge-worker=true
```

And, replace `__kubernetes_service_host__`, `__kubernetes_service_port_https__` with your `kube-apiserver` server address and port, then  apply the yurt-tunnel-agent yaml:

```bash
kubectl apply -f config/setup/yurt-tunnel-agent.yaml
```

After the agent and the server are running, we should execute the command in
the test-po again.
