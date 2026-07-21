#!/usr/bin/env bash
# deploy-blockpack.sh — build, push, and roll all blockpack-custom components.
#
# Usage:
#   ./deploy-blockpack.sh <revision>        # e.g. ./deploy-blockpack.sh r51
#   ./deploy-blockpack.sh                   # auto-increments from current deployed revision
#
# Rolls: block-builder (statefulset), backend-worker (statefulset), querier (deployment),
#        query-frontend (deployment), live-store-zone-a/b (statefulset),
#        value-index-compactor (statefulset), compaction-planner (deployment),
#        compaction-worker (deployment)
#
# job-planner was retired 2026-07-21 (issue #522): vi_backfill/cube_backfill's
# chain-continuation planning moved fully into blockpack's own compaction-planner, leaving
# job-planner with zero remaining responsibilities.

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

# Ensure Postgres exists before rolling components that depend on it -- issue #504 made
# cube's registry Postgres-only (validateConfig hard-fails at tempo startup if
# blockpack.cube_tenants is non-empty and storage.trace.postgres isn't configured). The
# .k8s/configs/{block-builder,backend-worker}.yaml templates below point at this instance;
# if it doesn't exist yet (e.g. a fresh namespace), a redeploy would otherwise crash-loop
# on startup the same way tempo-dev-test-03 did on 2026-07-15. Idempotent: kubectl apply
# no-ops if postgres-viusage already exists and is unchanged.
echo "--- Ensuring postgres-viusage exists ---"
cat <<'PGEOF' | kubectl apply -n "$NAMESPACE" -f -
apiVersion: v1
kind: Service
metadata:
  name: postgres-viusage
  labels:
    app: postgres-viusage
spec:
  clusterIP: None
  selector:
    app: postgres-viusage
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres-viusage
  labels:
    app: postgres-viusage
spec:
  serviceName: postgres-viusage
  replicas: 1
  selector:
    matchLabels:
      app: postgres-viusage
  template:
    metadata:
      labels:
        app: postgres-viusage
    spec:
      containers:
        - name: postgres
          image: postgres:16-alpine
          # max_connections=300 (2026-07-17): the default 100 was exhausted by this
          # namespace's own fleet -- 20 querier + 4 backend-worker + 20
          # value-index-compactor + backend-scheduler, each holding its own pgxpool,
          # sustained "FATAL: sorry, too many clients already" under normal load, not
          # a spike. shared_buffers bumped alongside it (rule of thumb ~a few MB of
          # overhead per connection); memory requests/limits raised to match.
          # args (not command!) so the image's default entrypoint (docker-entrypoint.sh) stays
          # in effect -- it drops root privileges before exec'ing postgres; overriding command
          # directly bypasses that and postgres refuses to run as root.
          args: ["-c", "max_connections=300", "-c", "shared_buffers=256MB"]
          env:
            - name: POSTGRES_HOST_AUTH_METHOD
              value: trust
            - name: PGDATA
              value: /var/lib/postgresql/data/pgdata
          ports:
            - name: postgres
              containerPort: 5432
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "postgres"]
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            exec:
              command: ["pg_isready", "-U", "postgres"]
            initialDelaySeconds: 15
            periodSeconds: 10
          resources:
            requests:
              cpu: 200m
              memory: 512Mi
            limits:
              cpu: 1000m
              memory: 1Gi
          volumeMounts:
            - name: data
              mountPath: /var/lib/postgresql/data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 5Gi
PGEOF
kubectl wait --for=condition=Ready pod/postgres-viusage-0 -n "$NAMESPACE" --timeout=120s

# Push configs from .k8s/configs/ reference files
CONFIGS_DIR="$(dirname "$0")/.k8s/configs"
echo "--- Pushing configs from ${CONFIGS_DIR} ---"
for component in block-builder backend-worker querier query-frontend live-store backend-scheduler; do
    cfg="${CONFIGS_DIR}/${component}.yaml"
    if [[ -f "$cfg" ]]; then
        kubectl patch configmap "tempo-${component}" -n "$NAMESPACE" --type=merge \
            -p "{\"data\":{\"tempo.yaml\":$(python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))' < "$cfg")}}"
        echo "    patched tempo-${component}"
    else
        echo "    WARNING: no config file for ${component} at ${cfg}"
    fi
done

# value-index-compactor's configmap ("value-index-compactor-config", key tempo.yaml) doesn't fit
# the tempo-${component} naming pattern above -- .k8s/configs/value-index-compactor.yaml is
# already the StatefulSet's own manifest, not its tempo.yaml content, so its config lives in a
# separately-named reference file instead. Wired with postgres: (2026-07-15) so its ManifestStore
# (issue #507) uses blockpack.NewPgColumnManifestStore instead of falling back to blob.
vic_compactor_cfg="${CONFIGS_DIR}/value-index-compactor-tempo.yaml"
if [[ -f "$vic_compactor_cfg" ]]; then
    kubectl patch configmap value-index-compactor-config -n "$NAMESPACE" --type=merge \
        -p "{\"data\":{\"tempo.yaml\":$(jq -Rs . < "$vic_compactor_cfg")}}"
    echo "    patched value-index-compactor-config"
else
    echo "    WARNING: no config file for value-index-compactor at ${vic_compactor_cfg}"
fi

# compaction-planner/compaction-worker (issue #522) are blockpack's own standalone binaries
# (built directly from the blockpack repo into bin/linux/compaction-{planner,worker}-$ARCH
# BEFORE this script runs -- see blockpack's own build step, not `make tempo` above), baked
# into this SAME tempo image by cmd/tempo/Dockerfile. Their ConfigMaps
# (compaction-planner-config/compaction-worker-config) and Deployments already exist from
# their initial rollout, so -- like querier/query-frontend below -- this script only ever
# patches the image and restarts, never re-applies the manifest wholesale.
echo "--- Updating compaction-planner ---"
kubectl set image deployment/compaction-planner -n "$NAMESPACE" "compaction-planner=${IMAGE}"
kubectl rollout restart deployment/compaction-planner -n "$NAMESPACE"

echo "--- Updating compaction-worker ---"
kubectl set image deployment/compaction-worker -n "$NAMESPACE" "compaction-worker=${IMAGE}"
kubectl rollout restart deployment/compaction-worker -n "$NAMESPACE"

# Roll components
echo "--- Updating backend-scheduler ---"
# Never rolled by this script before (2026-07-21 fix) despite its ConfigMap being patched in
# the loop above -- backend-scheduler owns CompactionProvider (the classic gRPC compaction
# candidate selector), so it needs the same image as backend-worker whenever compaction-path
# code changes (e.g. Encoding.CompactionSupported), not just when its own config changes.
kubectl set image statefulset/backend-scheduler -n "$NAMESPACE" "backend-scheduler=${IMAGE}"
kubectl delete pod backend-scheduler-0 -n "$NAMESPACE"

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
# Blockpack disk cache (file_cache_path: /var/tempo/blockpack-cache) — issue #515:
# was a 10Gi emptyDir (node-local disk, risked overwhelming node capacity across
# co-scheduled pods — same class of problem as #500). Now a 40Gi generic ephemeral
# volume (spec.volumes[].ephemeral.volumeClaimTemplate): a real PVC provisioned via
# the cluster's default StorageClass, off node-local disk, auto-deleted with its
# pod. 40Gi matches file_cache_max_bytes's own "32 GiB = 80% of 40 GiB" comment in
# querier.yaml (the OLD 10Gi here was already stale/inconsistent with that comment
# before this change). Does NOT survive pod recreation (cache is cold after every
# restart either way, same as emptyDir) — closing that gap relies on the memcache
# role-swap fix (querier.yaml's memcache_servers/metadata_memcache_servers), not on
# volume type. fsGroup:10001 matches cmd/tempo/Dockerfile's USER 10001:10001 — a
# fresh CSI-provisioned volume defaults to root:root and querier would get
# permission denied writing to /var/tempo/blockpack-cache without it.
# "emptyDir":null explicitly removes the old volume-source field — Kubernetes
# Volume is a union type; a strategic-merge patch that added "ephemeral" without
# nulling "emptyDir" would leave BOTH set on the same volume entry, which the API
# server rejects as invalid. Strategic merge patch is idempotent — safe to
# re-apply on every deploy.
kubectl patch deployment/querier -n "$NAMESPACE" --type=strategic -p \
    '{"spec":{"template":{"spec":{"securityContext":{"fsGroup":10001},"volumes":[{"name":"blockpack-cache","emptyDir":null,"ephemeral":{"volumeClaimTemplate":{"spec":{"accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"40Gi"}}}}}}],"containers":[{"name":"querier","volumeMounts":[{"name":"blockpack-cache","mountPath":"/var/tempo/blockpack-cache"}]}]}}}}'
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
# 2026-07-21: this StatefulSet no longer exists in the cluster (VI/VCNT/cube compaction
# already moved entirely to blockpack's own compaction-worker in an earlier #522 phase) --
# tolerate its absence instead of aborting the rest of the rollout, since set -e would
# otherwise kill this script the moment kubectl reports it missing.
if kubectl get statefulset/value-index-compactor -n "$NAMESPACE" >/dev/null 2>&1; then
    kubectl set image statefulset/value-index-compactor -n "$NAMESPACE" "value-index-compactor=${IMAGE}"
    kubectl rollout restart statefulset/value-index-compactor -n "$NAMESPACE"
else
    echo "    value-index-compactor no longer exists in this cluster -- skipping"
fi

# Wait for readiness
echo "--- Waiting for pods to be ready ---"
kubectl wait --for=condition=Ready pod/backend-scheduler-0 -n "$NAMESPACE" --timeout=120s
kubectl wait --for=condition=Ready pod/block-builder-0 -n "$NAMESPACE" --timeout=120s
kubectl wait --for=condition=Ready pod/backend-worker-0 -n "$NAMESPACE" --timeout=120s
kubectl rollout status deployment/querier -n "$NAMESPACE" --timeout=120s
kubectl rollout status deployment/query-frontend -n "$NAMESPACE" --timeout=120s
# compaction-planner/compaction-worker are plain Deployments (no ordinal pods, no sharding) --
# rollout status already waits for every replica, unlike a StatefulSet's own pod-0-only wait
# pattern elsewhere in this section (this project's standing "verify all replicas after
# deploy" rule).
kubectl rollout status deployment/compaction-planner -n "$NAMESPACE" --timeout=120s
kubectl rollout status deployment/compaction-worker -n "$NAMESPACE" --timeout=120s
kubectl wait --for=condition=Ready pod/live-store-zone-a-0 -n "$NAMESPACE" --timeout=120s
kubectl wait --for=condition=Ready pod/live-store-zone-b-0 -n "$NAMESPACE" --timeout=120s
# value-index-compactor has 20 replicas and is already known to be crash-looping (OOMKilled) --
# don't block the whole deploy on full-fleet readiness; just confirm pod-0 comes up on the new
# image so the rollout itself is verified, and let the caller inspect fleet health separately.
kubectl wait --for=condition=Ready pod/value-index-compactor-0 -n "$NAMESPACE" --timeout=120s || \
    echo "    WARNING: value-index-compactor-0 not Ready within 120s -- check fleet health separately"

echo ""
echo "==> Deploy complete: ${IMAGE}"
echo "    backend-scheduler:    $(kubectl get pod backend-scheduler-0 -n $NAMESPACE -o jsonpath='{.spec.containers[0].image}')"
echo "    block-builder:        $(kubectl get pod block-builder-0 -n $NAMESPACE -o jsonpath='{.spec.containers[0].image}')"
echo "    backend-worker:       $(kubectl get pod backend-worker-0 -n $NAMESPACE -o jsonpath='{.spec.containers[0].image}')"
echo "    querier:              $(kubectl get deployment querier -n $NAMESPACE -o jsonpath='{.spec.template.spec.containers[0].image}')"
echo "    query-frontend:       $(kubectl get deployment query-frontend -n $NAMESPACE -o jsonpath='{.spec.template.spec.containers[0].image}')"
echo "    compaction-planner:   $(kubectl get deployment compaction-planner -n $NAMESPACE -o jsonpath='{.spec.template.spec.containers[0].image}')"
echo "    compaction-worker:    $(kubectl get deployment compaction-worker -n $NAMESPACE -o jsonpath='{.spec.template.spec.containers[0].image}') ($(kubectl get pods -n $NAMESPACE -l app=compaction-worker --no-headers 2>/dev/null | grep -c Running || echo '?')/$(kubectl get deployment compaction-worker -n $NAMESPACE -o jsonpath='{.spec.replicas}') Running)"
echo "    live-store-a:         $(kubectl get pod live-store-zone-a-0 -n $NAMESPACE -o jsonpath='{.spec.containers[0].image}')"
echo "    live-store-b:         $(kubectl get pod live-store-zone-b-0 -n $NAMESPACE -o jsonpath='{.spec.containers[0].image}')"
echo "    value-index-compactor: $(kubectl get statefulset value-index-compactor -n $NAMESPACE -o jsonpath='{.spec.template.spec.containers[0].image}') ($(kubectl get pods -n $NAMESPACE -l app=value-index-compactor --no-headers 2>/dev/null | grep -c Running || echo '?')/$(kubectl get statefulset value-index-compactor -n $NAMESPACE -o jsonpath='{.spec.replicas}') Running)"
