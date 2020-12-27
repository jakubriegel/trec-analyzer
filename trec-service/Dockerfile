FROM debian:bullseye-slim as setup-stage

RUN apt update
RUN apt install -y git make gcc
RUN git clone https://github.com/usnistgov/trec_eval.git
WORKDIR trec_eval
RUN make

FROM gradle:6.7.1-jdk11 as build-stage

WORKDIR /app

COPY build.gradle.kts /app/build.gradle.kts
COPY settings.gradle.kts /app/settings.gradle.kts
COPY gradle /app/gradle
COPY src /app/src

COPY build /app/build
#RUN gradle build shadowJar

FROM ubuntu:20.04 as production-stage

RUN apt update
RUN apt install -y openjdk-11-jdk

COPY --from=setup-stage /trec_eval /trec_eval
COPY --from=build-stage /app/build/libs/trec-service.jar /trec-service.jar

COPY qrels /qrels
COPY topics /topics

EXPOSE 8001

CMD java -jar /trec-service.jar
