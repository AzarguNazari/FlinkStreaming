FROM openjdk:8 as build

# Install maven
RUN apt-get update
RUN apt-get install -y maven

WORKDIR /code

# Prepare by downloading dependencies
ADD pom.xml /code/pom.xml
RUN ["mvn", "dependency:resolve"]
#RUN ["mvn", "verify"]

# Adding source, compile and package into a fat jar
# This assumes you've configured such a goal in pom.xml
ADD src /code/src
RUN ["mvn", "package"]
CMD ["java", "-jar", "target/simpleJava-jar-with-dependencies.jar"]