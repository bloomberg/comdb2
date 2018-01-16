FROM openjdk:8

# Download maven
RUN \
  wget https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.5.0/apache-maven-3.5.0-bin.tar.gz && \
  tar -xvf apache-maven-3.5.0-bin.tar.gz -C /bin && \
  mv /bin/apache-maven-3.5.0 bin/maven

# Sets maven to use a useful settings.xml file
COPY \
  contrib/docker/maven-settings.xml /bin/maven/conf/settings.xml

# Install protobuf 3.2 and give all users exec access
RUN \
  wget https://github.com/google/protobuf/releases/download/v3.2.0/protoc-3.2.0-linux-x86_64.zip && \
  unzip protoc-3.2.0-linux-x86_64.zip -d protoc && \
  mv protoc/bin/protoc /bin && \
  chmod 755 /bin/protoc
