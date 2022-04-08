#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ -z $JAVA_HOME ]; then
   export JAVA_HOME=/home/scmtools/buildkit/java/jdk1.8.0_25
   export PATH=$JAVA_HOME/bin:$PATH
fi

if [ -z $MAVEN_HOME ]; then
   export MAVEN_HOME=/home/scmtools/buildkit/maven/apache-maven-3.3.9
   export PATH=$MAVEN_HOME/bin:$PATH
fi

TRAVIS_DIR=./computer-dist/src/assembly/travis
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hugegraph-client-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hugegraph-client -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/client-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hugegraph-common-1.8.10.jar -DgroupId=com.baidu.hugegraph -DartifactId=hugegraph-common -Dversion=1.8.10 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/common-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hugegraph-core-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hugegraph-core -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/hugegraph-core-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hugegraph-hstore-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hugegraph-hstore -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/hugegraph-hstore-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hugegraph-pom.xml -DgroupId=com.baidu.hugegraph -DartifactId=hugegraph -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/hugegraph-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hugegraph-loader-0.11.3.jar -DgroupId=com.baidu.hugegraph -DartifactId=hugegraph-loader -Dversion=0.11.3 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/hugegraph-loader-0.11.3-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hugegraph-plugin-1.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hugegraph-plugin -Dversion=1.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/hugegraph-plugin-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/syncgateway-1.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=syncgateway -Dversion=1.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/syncgateway-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hg-pd-client-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-pd-client -Dversion=3.0.0 -Dpackaging=jar  -DpomFile=$TRAVIS_DIR/lib/hg-pd-client-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hg-pd-common-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-pd-common -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/hg-pd-common-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hg-pd-grpc-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-pd-grpc -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/hg-pd-grpc-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hg-store-client-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-store-client -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/hg-store-client-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hg-store-grpc-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-store-grpc -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/hg-store-grpc-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hg-store-term-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-store-term -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/hg-store-term-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hadoop/hadoop-hdfs-client-2.7.2.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-hdfs-client -Dversion=2.7.2 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/hadoop/hadoop-hdfs-client-2.7.2-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/lib/hadoop/hadoop-hdfs-2.7.2.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-hdfs -Dversion=2.7.2 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/lib/hadoop/hadoop-hdfs-2.7.2-pom.xml