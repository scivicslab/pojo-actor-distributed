#!/bin/bash
# Build and test the four projects that are released with one version number, on the W206 k8s cluster
# (never on 5.12), in the order of their dependencies:
#   POJO-actor -> Turing-workflow -> (Turing-workflow-plugins) -> pojo-actor-distributed
# Each of the first three is installed into the pod's ~/.m2 from the HEAD of its git checkout, with its own
# tests (never -DskipTests; -Dgpg.skip=true only skips the signing, which needs a key the pod has not got).
# pojo-actor-distributed is built last from this working tree (src and pom.xml), so that a test that is not
# committed yet can be run; it is given the versions of the other two as -Dpojo-actor.version and
# -Dturing-workflow.version, so that it builds against what was just installed, not against Maven Central.
#
# Usage: [POD=name] [IMAGE=image] [WITH_PLUGINS=1] [POJO_ACTOR_DIR=dir] [TURING_WORKFLOW_DIR=dir]
#        [PLUGINS_DIR=dir] tasks/w206-chain-build.sh [mvn arguments for pojo-actor-distributed]   (default: test)
#   POD           the build pod to use or create (default: mvn-chain); every run replaces /work in the pod.
#                 Delete the pod when the work is done: kubectl delete pod $POD
#   IMAGE         the image a new pod starts from (default: ubuntu:24.04, which gets GraalVM CE and Maven
#                 installed in the pod; GraalVM CE is the JDK of every project here)
#   WITH_PLUGINS  1 also builds and tests Turing-workflow-plugins (about 5 more minutes); default: not built,
#                 pojo-actor-distributed does not depend on it
#   *_DIR         a git checkout to take HEAD from instead of ~/works/<project>, e.g. a throwaway clone
#                 with a branch merged that is not on main yet
set -eu
POD=${POD:-mvn-chain}
IMAGE=${IMAGE:-ubuntu:24.04}
GRAALVM_VERSION=25.0.0
MAVEN_VERSION=3.9.11
SRC=$(cd "$(dirname "$0")/.." && pwd)
W=$(cd "$SRC/.." && pwd)
POJO_ACTOR_DIR=${POJO_ACTOR_DIR:-$W/POJO-actor}
TURING_WORKFLOW_DIR=${TURING_WORKFLOW_DIR:-$W/Turing-workflow}
PLUGINS_DIR=${PLUGINS_DIR:-$W/Turing-workflow-plugins}
run() { kubectl exec "$POD" -- bash -c "$1"; }

# The version a pom.xml declares for itself (the first <version> directly under <project>).
pom_version() {
  python3 -c "import sys,xml.etree.ElementTree as E; print(E.parse(sys.argv[1]).getroot().find('{http://maven.apache.org/POM/4.0.0}version').text)" "$1"
}

if ! kubectl get pod "$POD" >/dev/null 2>&1; then
  kubectl run "$POD" --image="$IMAGE" --restart=Never \
    --overrides='{"spec":{"activeDeadlineSeconds":14400,"containers":[{"name":"'$POD'","image":"'$IMAGE'","command":["sleep","14400"],"resources":{"requests":{"cpu":"2"},"limits":{"cpu":"4"}}}]}}' >/dev/null
  kubectl wait --for=condition=Ready pod/"$POD" --timeout=300s
  # curl: some plugin tests run it; git: not needed in the pod because the sources are sent as tar archives
  run "set -e
    apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq curl ca-certificates >/dev/null
    mkdir -p /opt/graalvm /opt/maven /work
    curl -fsSL https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-$GRAALVM_VERSION/graalvm-community-jdk-${GRAALVM_VERSION}_linux-x64_bin.tar.gz | tar -xz -C /opt/graalvm --strip-components=1
    curl -fsSL https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz | tar -xz -C /opt/maven --strip-components=1
    ln -sf /opt/graalvm/bin/java /usr/local/bin/java; ln -sf /opt/maven/bin/mvn /usr/local/bin/mvn
    java -version 2>&1 | head -2"
fi

# install NAME DIR: send DIR's HEAD to the pod and run its tests and mvn install, with a time limit
install() {
  name=$1; dir=$2
  echo "== $name: HEAD $(git -C "$dir" rev-parse --short HEAD) of $dir"
  run "rm -rf /work/$name && mkdir -p /work/$name"
  (cd "$dir" && git archive HEAD) | kubectl exec -i "$POD" -- tar -x -C /work/$name
  run "cd /work/$name && find . -name target -type d -prune -exec rm -rf {} + ; timeout 3000 mvn -B install -Dgpg.skip=true > /work/$name.log 2>&1; echo $name MVN_EXIT=\$?; sed -n '/Tests run:.*Skipped: [0-9]*\$/p;/BUILD/p' /work/$name.log | tail -2"
}

install POJO-actor "$POJO_ACTOR_DIR"
install Turing-workflow "$TURING_WORKFLOW_DIR"
if [ "${WITH_PLUGINS:-0}" = 1 ]; then install Turing-workflow-plugins "$PLUGINS_DIR"; fi

PA_VERSION=$(pom_version "$POJO_ACTOR_DIR/pom.xml")
TW_VERSION=$(pom_version "$TURING_WORKFLOW_DIR/pom.xml")
echo "== pojo-actor-distributed: working tree of $SRC, against POJO-actor $PA_VERSION and Turing-workflow $TW_VERSION"
run "rm -rf /work/pojo-actor-distributed && mkdir -p /work/pojo-actor-distributed"
(cd "$SRC" && tar -c src pom.xml) | kubectl exec -i "$POD" -- tar -x -C /work/pojo-actor-distributed
run "cd /work/pojo-actor-distributed && rm -rf target && java -version 2>&1 | head -2 && timeout 3000 mvn -B ${*:-test} -Dgpg.skip=true -Dpojo-actor.version=$PA_VERSION -Dturing-workflow.version=$TW_VERSION > /work/pojo-actor-distributed.log 2>&1; echo pojo-actor-distributed MVN_EXIT=\$?; sed -n '/Tests run:.*Skipped: [0-9]*\$/p;/BUILD/p;/^\[ERROR\]/p' /work/pojo-actor-distributed.log | tail -40"
