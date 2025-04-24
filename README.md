Init cluster using 'Kind', Ref: https://kind.sigs.k8s.io/
```bash
kind create cluster --config kind-config.yaml --name custom-scheduler
```


Development:
```bash
VERSION=0.0.2 && \
docker build -t custom-scheduler:${VERSION} . && \
kind load docker-image custom-scheduler:${VERSION} --name custom-scheduler
```