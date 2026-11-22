#!/bin/bash
# Build and test pojo-actor-distributed on the W206 k8s cluster (never on 5.12).
# Usage: [POD=name] [IMAGE=image] tasks/w206-build.sh [mvn args...]   (default: test)
#   POD    the build pod to use or create (default: mvn-pad). Use a pod of your own when
#          others build at the same time: every run replaces /work in the pod.
#   IMAGE  the image a new pod starts from (default: maven:3.9-eclipse-temurin-21). An image
#          without mvn (e.g. ubuntu:24.04) gets GraalVM CE and Maven installed in the pod.
# Never pass -DskipTests.
set -eu
POD=${POD:-mvn-pad}
IMAGE=${IMAGE:-maven:3.9-eclipse-temurin-21}
GRAALVM_VERSION=25.0.0
MAVEN_VERSION=3.9.11
M2=$HOME/.m2/repository/com/scivicslab
SRC=$(cd "$(dirname "$0")/.." && pwd)
if ! kubectl get pod $POD >/dev/null 2>&1; then
  kubectl run $POD --image=$IMAGE --restart=Never \
    --overrides='{"spec":{"activeDeadlineSeconds":14400,"containers":[{"name":"'$POD'","image":"'$IMAGE'","command":["sleep","14400"],"resources":{"requests":{"cpu":"1"},"limits":{"cpu":"4"}}}]}}' >/dev/null
  kubectl wait --for=condition=Ready pod/$POD --timeout=180s
  if ! kubectl exec $POD -- sh -c 'command -v mvn' >/dev/null 2>&1; then
    kubectl exec $POD -- bash -c "set -e
      apt-get update -qq && apt-get install -y -qq curl ca-certificates git >/dev/null
      mkdir -p /opt/graalvm /opt/maven
      curl -fsSL https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-$GRAALVM_VERSION/graalvm-community-jdk-${GRAALVM_VERSION}_linux-x64_bin.tar.gz \
        | tar -xz -C /opt/graalvm --strip-components=1
      curl -fsSL https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz \
        | tar -xz -C /opt/maven --strip-components=1
      ln -sf /opt/graalvm/bin/java /usr/local/bin/java
      ln -sf /opt/maven/bin/mvn /usr/local/bin/mvn"
  fi
  kubectl exec $POD -- mkdir -p /root/.m2/repository/com/scivicslab /work
  for a in POJO-actor pojo-actor turing-workflow; do
    kubectl exec $POD -- mkdir -p /root/.m2/repository/com/scivicslab/$a
    [ -d $M2/$a/4.0.0 ] && kubectl cp $M2/$a/4.0.0 $POD:/root/.m2/repository/com/scivicslab/$a/4.0.0
  done
fi
kubectl exec $POD -- rm -rf /work/src /work/pom.xml /work/target
kubectl cp "$SRC/src" $POD:/work/src
kubectl cp "$SRC/pom.xml" $POD:/work/pom.xml
kubectl exec $POD -- bash -c "cd /work && rm -rf target && java -version 2>&1 | head -2 && mvn -B -q ${*:-test} 2>&1 | tail -80; echo MVN_EXIT=\${PIPESTATUS[0]}"
