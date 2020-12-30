FROM ubuntu:20.04 as setup-stage

# trec_eval setup
RUN apt update
RUN apt install -y git make gcc
RUN git clone https://github.com/usnistgov/trec_eval.git
WORKDIR trec_eval
RUN make

# data setup
COPY corpus /corpus
COPY qrels /qrels
COPY topics /topics
RUN mkdir /terrier_data

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
COPY --from=setup-stage /corpus /corpus
COPY --from=setup-stage /qrels /qrels
COPY --from=setup-stage /topics /topics
COPY --from=setup-stage /terrier_data /terrier_data

COPY config/application.conf /application.conf
COPY --from=build-stage /app/build/libs/trec-service.jar /trec-service.jar

EXPOSE 8001

CMD java -jar /trec-service.jar
