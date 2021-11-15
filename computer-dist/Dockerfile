FROM openjdk:8
LABEL maintainer="HugeGraph Docker Maintainers <hugegraph@googlegroups.com>"
ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -XX:MaxRAMFraction=2 -XshowSettings:vm"
COPY . /etc/local/hugegraph-computer
WORKDIR /etc/local/hugegraph-computer
RUN apt-get update && apt-get -y install gettext-base && apt-get -y install wget && apt-get -y install net-tools && apt-get clean
RUN wget https://arthas.aliyun.com/arthas-boot.jar
