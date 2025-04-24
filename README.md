## Init cluster using 'Kind', Ref: https://kind.sigs.k8s.io/
```bash
kind create cluster --config kind-config.yaml --name custom-scheduler
```

## Optional:
#### install metrics server
```bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml && \
kubectl patch deployment metrics-server -n kube-system \
  --type='json' -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-", "value":"--kubelet-insecure-tls"}]'
```


## Development:
```bash
cd custom-scheduler
```
```bash
VERSION={x.y.z} && \
docker build -t custom-scheduler:${VERSION} . && \
kind load docker-image custom-scheduler:${VERSION} --name custom-scheduler
```


## Scheduler Deployment:
```bash
# update scheduler image tag in the yaml file and apply
kubectl apply -f scheduler-deployment.yaml
```
