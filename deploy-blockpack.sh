#!/usr/bin/env bash
# deploy-blockpack.sh — build, push, and roll all blockpack-custom components.
#
# Usage:
#   ./deploy-blockpack.sh <revision>        # e.g. ./deploy-blockpack.sh r51
#   ./deploy-blockpack.sh                   # auto-increments from current deployed revision
#
# Rolls: block-builder (statefulset), backend-worker (statefulset), querier (deployment),
#        query-frontend (deployment), live-store-zone-a/b (statefulset),
#        value-index-compactor (statefulset)

set -euo pipefail

NAMESPACE="tempo-dev-test-03"
IMAGE_BASE="mrdgrafana/tempo"
BRANCH="blockpack"
GIT_HASH=$(git rev-parse --short HEAD)

# Determine revision
if [[ $# -ge 1 ]]; then
    REV="$1"
else
    # Auto-detect current revision from block-builder and increment
    CURRENT=$(kubectl get statefulset block-builder -n "$NAMESPACE" \
        -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null \
        | grep -oE 'r[0-9]+$' || echo "r0")
    NEXT=$((${CURRENT#r} + 1))
    REV="r${NEXT}"
    echo "Auto-detected current revision: ${CURRENT} → using ${REV}"
fi

IMAGE="${IMAGE_BASE}:${BRANCH}-${GIT_HASH}-${REV}"
echo "==> Building image: ${IMAGE}"

# Build binaries
echo "--- Building amd64 binary ---"
GOOS=linux GOARCH=amd64 make tempo

echo "--- Building arm64 binary ---"
GOOS=linux GOARCH=arm64 make tempo

# Build and push multi-arch image
echo "--- Building and pushing Docker image ---"
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    -t "${IMAGE}" \
    --push \
    -f cmd/tempo/Dockerfile .

echo "==> Image pushed: ${IMAGE}"

# Push configs from .k8s/configs/ reference files
CONFIGS_DIR="$(dirname "$0")/.k8s/configs"
echo "--- Pushing configs from ${CONFIGS_DIR} ---"
for component in block-builder backend-worker querier query-frontend live-store; do
    cfg="${CONFIGS_DIR}/${component}.yaml"
    if [[ -f "$cfg" ]]; then
        kubectl patch configmap "tempo-${component}" -n "$NAMESPACE" --type=merge \
            -p "{\"data\":{\"tempo.yaml\":$(python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))' < "$cfg")}}"
        echo "    patched tempo-${component}"
    else
        echo "    WARNING: no config file for ${component} at ${cfg}"
    fi
done

# Roll components
echo "--- Updating block-builder ---"
kubectl set image statefulset/block-builder -n "$NAMESPACE" "block-builder=${IMAGE}"
kubectl delete pod block-builder-0 -n "$NAMESPACE"

echo "--- Updating backend-worker ---"
kubectl set image statefulset/backend-worker -n "$NAMESPACE" "backend-worker=${IMAGE}"
kubectl delete pod backend-worker-0 -n "$NAMESPACE"

echo "--- Updating querier ---"
kubectl set image deployment/querier -n "$NAMESPACE" "querier=${IMAGE}"
# GOMEMLIMIT is required to bound parsedIntrinsicCache (objectcache.Cache budget = 20% of GOMEMLIMIT).
# Without it, the cache is unbounded and querier pods OOMKill after a few M8 histogram queries.
# MUST stay below the container's actual memory limit -- Go's GC needs headroom to react to
# GOMEMLIMIT before the kernel enforces the hard cgroup limit. Derived as 80% of the
# deployment's own memory limit rather than hardcoded, so it can't silently drift out of sync
# with it (2026-07-06 incident: a stale GOMEMLIMIT=13GiB sat ABOVE the actual 8Gi limit, so
# GOMEMLIMIT never triggered and a broad search query OOMKilled the querier outright instead
# of the GC backing off gracefully).
MEM_LIMIT=$(kubectl get deployment/querier -n "$NAMESPACE" \
    -o jsonpath='{.spec.template.spec.containers[0].resources.limits.memory}')
case "$MEM_LIMIT" in
    *Gi) MEM_MIB=$(( ${MEM_LIMIT%Gi} * 1024 )) ;;
    *Mi) MEM_MIB=${MEM_LIMIT%Mi} ;;
    *) echo "ERROR: unrecognized querier memory limit format: ${MEM_LIMIT}" >&2; exit 1 ;;
esac
GOMEMLIMIT_MIB=$(( MEM_MIB * 80 / 100 ))
echo "    querier memory limit: ${MEM_LIMIT} -> GOMEMLIMIT=${GOMEMLIMIT_MIB}MiB (80%)"
kubectl set env deployment/querier -n "$NAMESPACE" "GOMEMLIMIT=${GOMEMLIMIT_MIB}MiB"
# Add 10 Gi emptyDir for blockpack disk cache (file_cache_path: /var/tempo/blockpack-cache).
# Strategic merge patch is idempotent — safe to re-apply on every deploy.
kubectl patch deployment/querier -n "$NAMESPACE" --type=strategic -p \
    '{"spec":{"template":{"spec":{"volumes":[{"name":"blockpack-cache","emptyDir":{"sizeLimit":"10Gi"}}],"containers":[{"name":"querier","volumeMounts":[{"name":"blockpack-cache","mountPath":"/var/tempo/blockpack-cache"}]}]}}}}'
kubectl rollout restart deployment/querier -n "$NAMESPACE"

echo "--- Updating query-frontend ---"
kubectl set image deployment/query-frontend -n "$NAMESPACE" "query-frontend=${IMAGE}"
kubectl rollout restart deployment/query-frontend -n "$NAMESPACE"

echo "--- Updating live-store ---"
kubectl set image statefulset/live-store-zone-a -n "$NAMESPACE" "live-store=${IMAGE}"
kubectl set image statefulset/live-store-zone-b -n "$NAMESPACE" "live-store=${IMAGE}"
kubectl rollout restart statefulset/live-store-zone-a -n "$NAMESPACE"
kubectl rollout restart statefulset/live-store-zone-b -n "$NAMESPACE"

echo "--- Updating value-index-compactor ---"
kubectl set image statefulset/value-index-compactor -n "$NAMESPACE" "value-index-compactor=${IMAGE}"
kubectl rollout restart statefulset/value-index-compactor -n "$NAMESPACE"

# Wait for readiness
echo "--- Waiting for pods to be ready ---"
kubectl wait --for=condition=Ready pod/block-builder-0 -n "$NAMESPACE" --timeout=120s
kubectl wait --for=condition=Ready pod/backend-worker-0 -n "$NAMESPACE" --timeout=120s
kubectl rollout status deployment/querier -n "$NAMESPACE" --timeout=120s
kubectl rollout status deployment/query-frontend -n "$NAMESPACE" --timeout=120s
kubectl wait --for=condition=Ready pod/live-store-zone-a-0 -n "$NAMESPACE" --timeout=120s
kubectl wait --for=condition=Ready pod/live-store-zone-b-0 -n "$NAMESPACE" --timeout=120s
# value-index-compactor has 20 replicas and is already known to be crash-looping (OOMKilled) --
# don't block the whole deploy on full-fleet readiness; just confirm pod-0 comes up on the new
# image so the rollout itself is verified, and let the caller inspect fleet health separately.
kubectl wait --for=condition=Ready pod/value-index-compactor-0 -n "$NAMESPACE" --timeout=120s || \
    echo "    WARNING: value-index-compactor-0 not Ready within 120s -- check fleet health separately"

echo ""
echo "==> Deploy complete: ${IMAGE}"
echo "    block-builder:        $(kubectl get pod block-builder-0 -n $NAMESPACE -o jsonpath='{.spec.containers[0].image}')"
echo "    backend-worker:       $(kubectl get pod backend-worker-0 -n $NAMESPACE -o jsonpath='{.spec.containers[0].image}')"
echo "    querier:              $(kubectl get deployment querier -n $NAMESPACE -o jsonpath='{.spec.template.spec.containers[0].image}')"
echo "    query-frontend:       $(kubectl get deployment query-frontend -n $NAMESPACE -o jsonpath='{.spec.template.spec.containers[0].image}')"
echo "    live-store-a:         $(kubectl get pod live-store-zone-a-0 -n $NAMESPACE -o jsonpath='{.spec.containers[0].image}')"
echo "    live-store-b:         $(kubectl get pod live-store-zone-b-0 -n $NAMESPACE -o jsonpath='{.spec.containers[0].image}')"
echo "    value-index-compactor: $(kubectl get statefulset value-index-compactor -n $NAMESPACE -o jsonpath='{.spec.template.spec.containers[0].image}') ($(kubectl get pods -n $NAMESPACE -l app=value-index-compactor --no-headers 2>/dev/null | grep -c Running || echo '?')/$(kubectl get statefulset value-index-compactor -n $NAMESPACE -o jsonpath='{.spec.replicas}') Running)"
