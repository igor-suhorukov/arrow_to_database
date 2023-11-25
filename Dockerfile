FROM adoptopenjdk/maven-openjdk11
ADD pom.xml /arrow_to_database/pom.xml
ADD src /arrow_to_database/src
RUN cd /arrow_to_database && mvn package
RUN mv /arrow_to_database/target/arrow_to_database-1.0-SNAPSHOT.jar /arrow_to_database.jar
RUN rm -rf /arrow_to_database/*

ENTRYPOINT ["java","-jar","/arrow_to_database.jar"]