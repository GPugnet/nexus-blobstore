ARG NEXUS_VERSION=3.10.0

FROM maven:3-jdk-8-alpine AS build
ARG NEXUS_VERSION=3.10.0
ARG NEXUS_BUILD=04

# copy everything in a folder
COPY . /nexus-blobstoreazurestorage/
# replace the dummy version number in the pom file with the one provided 
# then compile and package it
RUN cd /nexus-blobstoreazurestorage/; sed -i "s/3.10.0-04/${NEXUS_VERSION}-${NEXUS_BUILD}/g" pom.xml; \
    mvn clean package;

FROM sonatype/nexus3:$NEXUS_VERSION
ARG NEXUS_VERSION=3.10.0
ARG NEXUS_BUILD=04
ARG PLUGIN_VERSION=1.0.0-SNAPSHOT
ARG NEXUS_HOME=/opt/sonatype/nexus
ARG TARGET_DIR=/opt/sonatype/nexus/system/org/sonatype/nexus/nexus-blobstoreazurestorage/${PLUGIN_VERSION}/
ARG DEPLOY_DIR=/opt/sonatype/nexus
USER root

RUN mkdir -p ${TARGET_DIR}; 
#\
#sed -i.bak -e "/nexus-blobstore-file/a\\"$'\n'"<bundle>mvn:org.sonatype.nexus/nexus-blobstore-azurestorage/${PLUGIN_VERSION}</bundle>" /opt/sonatype/nexus/system/org/sonatype/nexus/assemblies/nexus-base-feature/*/nexus-base-feature-*-features.xml \
#sed -i.bak -e "/nexus-blobstore-file/a\\"$'\n'"<bundle>mvn:org.sonatype.nexus/nexus-blobstore-azurestorage/${PLUGIN_VERSION}</bundle>" /opt/sonatype/nexus/system/org/sonatype/nexus/assemblies/nexus-core-feature/*/nexus-core-feature-*-features.xml 
COPY --from=build /nexus-blobstoreazurestorage/target/nexus-blobstoreazurestorage-${PLUGIN_VERSION}.jar ${DEPLOY_DIR}
USER nexus