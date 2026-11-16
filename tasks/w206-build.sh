#!/bin/bash
# Build and test pojo-actor-distributed on the W206 k8s cluster (never on 5.12).
# Usage: tasks/w206-build.sh [mvn args...]   (default: test)
# Never pass -DskipTests.
set -eu
POD=mvn-pad
M2=$HOME/.m2/repository/com/scivicslab
SRC=$(cd "$(dirname "$0")/.." && pwd)
if ! kubectl get pod $POD >/dev/null 2>&1; then
  kubectl run $POD --image=maven:3.9-eclipse-temurin-21 --restart=Never \
    --overrides='{"spec":{"activeDeadlineSeconds":14400,"containers":[{"name":"mvn-pad","image":"maven:3.9-eclipse-temurin-21","command":["sleep","14400"],"resources":{"requests":{"cpu":"1"},"limits":{"cpu":"4"}}}]}}' >/dev/null
  kubectl wait --for=condition=Ready pod/$POD --timeout=180s
  kubectl exec $POD -- mkdir -p /root/.m2/repository/com/scivicslab /work
  for a in POJO-actor pojo-actor turing-workflow; do
    kubectl exec $POD -- mkdir -p /root/.m2/repository/com/scivicslab/$a
    [ -d $M2/$a/4.0.0 ] && kubectl cp $M2/$a/4.0.0 $POD:/root/.m2/repository/com/scivicslab/$a/4.0.0
  done
fi
kubectl exec $POD -- rm -rf /work/src /work/pom.xml /work/target
kubectl cp "$SRC/src" $POD:/work/src
kubectl cp "$SRC/pom.xml" $POD:/work/pom.xml
kubectl exec $POD -- bash -c "cd /work && rm -rf target && mvn -B -q ${*:-test} 2>&1 | tail -80; echo MVN_EXIT=\${PIPESTATUS[0]}"
