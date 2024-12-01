FROM ubuntu:latest
LABEL authors="aashr"

ENTRYPOINT ["top", "-b"]
FROM apache/airflow:2.7.0-python3.9

# Install OpenJDK 11
RUN apt-get update && apt-get install -y openjdk-11-jdk

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
