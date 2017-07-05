#!/bin/bash

set -e
git pull

mvn clean install -DskipTests

VERSION=`grep -o -a -m 1 -h -r "version>.*</version" ./pom.xml | head -1 | sed "s/version//g" | sed "s/>//" | sed "s/<\///g"`

echo ""
echo "Deploying version: $VERSION ... to maven repository"
echo ""
mvn  deploy:deploy-file -Durl=scpexe://kompics.i.sics.se/home/maven/repository \
                      -DrepositoryId=sics-release-repository \
                      -Dfile=./target/hops-tensorflow-${VERSION}.jar \
                      -DgroupId=io.hops \
                      -DartifactId=hops-tensorflow \
                      -Dversion=${VERSION} \
                      -Dpackaging=jar \
                      -DpomFile=./pom.xml \
-DgeneratePom.description="Hops Tensorflow"

echo "Compiling for snurran deployment"
mvn clean package -Phops -DskipTests

echo ""
echo "Deploying hops-tensorflow-${VERSION}.jar to snurran.sics.se"
echo ""
scp target/hops-tensorflow-${VERSION}.jar glassfish@snurran.sics.se:/var/www/hops

