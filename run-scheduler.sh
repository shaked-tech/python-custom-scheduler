#!/bin/bash

set -e

## Init cluster using 'Kind', Ref: https://kind.sigs.k8s.io/
kind create cluster --config kind-config.yaml --name custom-scheduler

VERSION=${1:-"stable"}
docker build -t custom-scheduler:${VERSION} custom-scheduler
kind load docker-image custom-scheduler:${VERSION} --name custom-scheduler

## Scheduler Deployment:
kubectl apply -f custom-scheduler/scheduler-deployment.yaml
