FROM maven:3.5-jdk-8 AS build

RUN apt-get upgrade
COPY . /usr/src/app/deequ
RUN mvn -f /usr/src/app/deequ/pom.xml clean install -DargLine="-Xms128m -Xms512m -XX:MaxPermSize=300m -ea"
