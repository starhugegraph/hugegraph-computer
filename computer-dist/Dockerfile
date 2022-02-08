FROM openjdk:8-jdk
COPY --from=hengyunabc/arthas:latest /opt/arthas /opt/arthas

LABEL maintainer="HugeGraph Docker Maintainers <hugegraph@googlegroups.com>"

ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -XX:MaxRAMFraction=2 -XshowSettings:vm"

ARG WORK_PATH="/opt/arthas"
COPY . ${WORK_PATH}

WORKDIR ${WORK_PATH}
RUN apt-get update && apt-get -y install gettext-base wget
RUN wget https://github.com/alibaba/arthas/releases/download/arthas-all-3.5.5/arthas-bin.zip
RUN unzip arthas-bin.zip