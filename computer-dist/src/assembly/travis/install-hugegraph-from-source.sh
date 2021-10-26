#!/usr/bin/env bash

set -ev

if [[ $# -ne 1 ]]; then
    echo "Must pass commit id of hugegraph repo"
    exit 1
fi

COMMIT_ID=$1
HUGEGRAPH_GIT_URL="https://github.com/starhugegraph/hugegraph.git"

git clone --depth 100 ${HUGEGRAPH_GIT_URL} #-b "${BRANCH}"
cd hugegraph
git checkout -b "${BRANCH}"
#git checkout "${COMMIT_ID}"
mvn package -DskipTests
mv hugegraph-*.tar.gz ../
cd ../
rm -rf hugegraph
tar -zxf hugegraph-*.tar.gz

cd "$(find hugegraph-* | head -1)"
sed -i "s/rpc.server_port=.*/rpc.server_port=8390/g" conf/rest-server.properties
bin/init-store.sh || exit 1
bin/start-hugegraph.sh || exit 1
cd ../
