# Kubernetes deployment

## Development/lab deployment

Build and load the image into a local Kind cluster:

```bash
docker build -t messlessmq:integrated .
kind load docker-image messlessmq:integrated --name messlessmq
kubectl apply -k deploy/kubernetes/
```

The StatefulSet creates three brokers with stable ordinal DNS names and one PVC per pod. `messlessmq-headless` is used for peer discovery; `messlessmq` is a ClusterIP bootstrap service for clients. The admin port is not exposed through either Service. Liveness/readiness use the local admin HTTP health endpoints; readiness is not a cluster quorum check.

Check the StorageClass and PVCs:

```bash
kubectl get storageclass
kubectl -n messlessmq get pods,pvc,svc
```

## Secure overlay

For TLS/mTLS, API-key AUTH and admin Bearer-key protection, follow [../../docs/SECURITY.md](../../docs/SECURITY.md) to generate local development certificates and create the `messlessmq-tls` and `messlessmq-auth` Kubernetes Secrets. Then apply:

```bash
kubectl apply -k deploy/kubernetes/secure/
```

Do not use the generated development CA in production. Use your organization's PKI, unique broker certificates, secret management, network policies, tested storage, resource sizing and backup/restore procedures.
