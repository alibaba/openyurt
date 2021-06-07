/*
Copyright 2020 The OpenYurt Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package constants

const (
	YurttunnelServerClusterRole = `
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  annotations:
    rbac.authorization.kubernetes.io/autoupdate: "true"
  name: yurt-tunnel-server
rules:
- apiGroups:
  - certificates.k8s.io
  resources:
  - certificatesigningrequests
  - certificatesigningrequests/approval
  verbs:
  - create
  - get
  - list
  - watch
  - delete
  - update
  - patch
- apiGroups:
  - certificates.k8s.io
  resources:
  - signers
  resourceNames:
  - "kubernetes.io/legacy-unknown"
  verbs:
  - approve
- apiGroups:
  - ""
  resources:
  - endpoints
  verbs:
  - get
- apiGroups:
  - ""
  resources:
  - nodes
  verbs:
  - list
  - watch
- apiGroups:
  - ""
  resources:
  - services
  verbs:
  - get
  - list
  - watch
- apiGroups:
  - ""
  resources:
  - configmaps
  verbs:
  - list
  - watch
  - get
  - create
  - update
- apiGroups:
  - "coordination.k8s.io"
  resources:
  - leases
  verbs:
  - create
  - get
  - update
`
	YurttunnelServerServiceAccount = `
apiVersion: v1
kind: ServiceAccount
metadata:
  name: yurt-tunnel-server
  namespace: kube-system
`
	YurttunnelServerClusterRolebinding = `
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: yurt-tunnel-server
subjects:
  - kind: ServiceAccount
    name: yurt-tunnel-server
    namespace: kube-system
roleRef:
  kind: ClusterRole
  name: yurt-tunnel-server
  apiGroup: rbac.authorization.k8s.io
`
	YurttunnelServerService = `
apiVersion: v1
kind: Service
metadata:
  name: x-tunnel-server-svc
  namespace: kube-system
  labels:
    name: yurt-tunnel-server
spec:
  type: NodePort 
  ports:
  - port: 10263
    targetPort: 10263
    name: https
  - port: 10262
    targetPort: 10262
    name: tcp
  selector:
    k8s-app: yurt-tunnel-server
`
	YurttunnelServerInternalService = `
apiVersion: v1
kind: Service
metadata:
  name: x-tunnel-server-internal-svc
  namespace: kube-system
  labels:
    name: yurt-tunnel-server
spec:
  ports:
    - port: 10250
      targetPort: 10263
      name: https
    - port: 10255
      targetPort: 10264
      name: http
  selector:
    k8s-app: yurt-tunnel-server
`

	YurttunnelServerConfigMap = `
apiVersion: v1
kind: ConfigMap
metadata:
  name: yurt-tunnel-server-cfg
  namespace: kube-system
data:
  dnat-ports-pair: ""
`
	YurttunnelServerDeployment = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: yurt-tunnel-server
  namespace: kube-system
  labels:
    k8s-app: yurt-tunnel-server
spec:
  replicas: 1
  selector:
    matchLabels:
      k8s-app: yurt-tunnel-server
  template:
    metadata:
      labels:
        k8s-app: yurt-tunnel-server
    spec:
      hostNetwork: true
      serviceAccountName: yurt-tunnel-server
      restartPolicy: Always
      volumes:
      - name: tunnel-server-dir
        hostPath:
          path: /var/lib/yurttunnel-server
          type: DirectoryOrCreate
      tolerations:
      - operator: "Exists"
      nodeSelector:
        beta.kubernetes.io/arch: amd64
        beta.kubernetes.io/os: linux
        {{.edgeWorkerLabel}}: "false"
      containers:
      - name: yurt-tunnel-server
        image: {{.image}} 
        imagePullPolicy: Always
        command:
        - yurt-tunnel-server
        args:
        - --bind-address=$(NODE_IP)
        - --insecure-bind-address=$(NODE_IP)
        - --server-count=1
        env:
        - name: NODE_IP
          valueFrom:
            fieldRef:
              fieldPath: status.hostIP
        securityContext:
          capabilities:
            add: ["NET_ADMIN", "NET_RAW"]
        volumeMounts:
        - name: tunnel-server-dir
          mountPath: /var/lib/yurttunnel-server
`
)
