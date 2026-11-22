#!/bin/bash
# Runs the Kafka integration tests (*IT.java) against a real broker on the W206 k8s cluster.
# Usage: tasks/kafka-it.sh [mvn args...]
#   default mvn args: verify -Dgpg.skip=true -Dkafka.it.bootstrap=<the broker>
#   KEEP=1  leave the namespace for another run (development only; delete it by hand afterwards
#           with `kubectl delete ns pad-kafka-it`). Without it the namespace is always deleted.
#
# This script runs on 5.12 and uses only kubectl. It:
#   1. creates the namespace pad-kafka-it, the broker (src/it/k8s/kafka.yaml) and the build pod
#      mvn-c84 with its ServiceAccount and Role (src/it/k8s/build-pod.yaml), and waits for both;
#   2. installs GraalVM CE and Maven in the build pod (ubuntu:24.04), copies the sources and the
#      POJO-actor / turing-workflow artifacts from ~/.m2;
#   3. runs `rm -rf target` and `mvn verify` in the pod; the property kafka.it.bootstrap turns on the
#      profile kafka-it, which runs failsafe; the reports are copied to target/kafka-it-reports/;
#   4. deletes the namespace on exit, also when a step failed (trap).
#
# Departure from TestingStandard_260404_oo01: the standard has Maven set up and tear down the
# environment (pre-integration-test / post-integration-test). Here the setup and teardown sit outside
# Maven, in this script, because Maven must not run on 5.12, and the broker is created by kubectl
# from 5.12 before Maven starts in the pod.
#
# Never pass -DskipTests. -Dgpg.skip=true skips signing only.
set -eu
NS=pad-kafka-it
POD=mvn-c84
GRAALVM_VERSION=25.0.0
MAVEN_VERSION=3.9.11
M2=$HOME/.m2/repository/com/scivicslab
SRC=$(cd "$(dirname "$0")/.." && pwd)
BOOTSTRAP=kafka-0.kafka.$NS.svc.cluster.local:9092
MVN_ARGS=${*:-verify -Dgpg.skip=true -Dkafka.it.bootstrap=$BOOTSTRAP}
REPORTS=$SRC/target/kafka-it-reports
K="kubectl -n $NS"

cleanup() {
  status=$?
  if [ "${KEEP:-0}" = 1 ]; then
    echo "KEEP=1: namespace $NS is left; delete it with: kubectl delete ns $NS"
  else
    echo "Deleting namespace $NS"
    kubectl delete ns $NS --wait=true --timeout=300s || echo "WARNING: could not delete namespace $NS"
  fi
  exit $status
}
trap cleanup EXIT

if ! kubectl get ns $NS >/dev/null 2>&1; then
  kubectl create ns $NS
fi
$K apply -f "$SRC/src/it/k8s/kafka.yaml"
$K apply -f "$SRC/src/it/k8s/build-pod.yaml"
$K rollout status statefulset/kafka --timeout=300s
$K wait --for=condition=Ready pod/$POD --timeout=300s

if ! $K exec $POD -- sh -c 'command -v mvn' >/dev/null 2>&1; then
  $K exec $POD -- bash -c "set -e
    apt-get update -qq && apt-get install -y -qq curl ca-certificates >/dev/null
    mkdir -p /opt/graalvm /opt/maven
    curl -fsSL https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-$GRAALVM_VERSION/graalvm-community-jdk-${GRAALVM_VERSION}_linux-x64_bin.tar.gz \
      | tar -xz -C /opt/graalvm --strip-components=1
    curl -fsSL https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz \
      | tar -xz -C /opt/maven --strip-components=1
    ln -sf /opt/graalvm/bin/java /usr/local/bin/java
    ln -sf /opt/maven/bin/mvn /usr/local/bin/mvn"
  for a in POJO-actor pojo-actor turing-workflow; do
    $K exec $POD -- mkdir -p /root/.m2/repository/com/scivicslab/$a
    [ -d $M2/$a/4.0.0 ] && $K cp $M2/$a/4.0.0 $POD:/root/.m2/repository/com/scivicslab/$a/4.0.0
  done
fi

$K exec $POD -- bash -c "rm -rf /work && mkdir -p /work"
$K cp "$SRC/src" $POD:/work/src
$K cp "$SRC/pom.xml" $POD:/work/pom.xml
set +e
$K exec $POD -- bash -c "cd /work && rm -rf target && java -version 2>&1 | head -2 && mvn -B $MVN_ARGS > /work/mvn.log 2>&1; echo \$? > /work/mvn.exit"
set -e
rm -rf "$REPORTS" && mkdir -p "$REPORTS"
$K cp $POD:/work/mvn.log "$REPORTS/mvn.log"
for d in surefire-reports failsafe-reports; do
  $K exec $POD -- test -d /work/target/$d && $K cp $POD:/work/target/$d "$REPORTS/$d" || true
done
MVN_EXIT=$($K exec $POD -- cat /work/mvn.exit)
echo "MVN_EXIT=$MVN_EXIT  (full log: $REPORTS/mvn.log)"
[ "$MVN_EXIT" = 0 ]
