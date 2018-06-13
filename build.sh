#!/usr/bin/env bash


#### Create tmp folder  ###

mkdir tmp/
cd tmp/

##### Build Repo Archive ####

git clone -b new-pride https://github.com/PRIDE-Archive/data-provider-api
mvn clean install -f data-provider-api/pom.xml -P oracle-pridedb-prod-machine,oracle-pridedb-ro-user

##### Build Repo Archive ####

git clone -b spring-boot https://github.com/PRIDE-Archive/repo
mvn clean install -f repo/pom.xml -P oracle-pridedb-prod-machine,oracle-pridedb-ro-user

#### Build MongoDB Dependency ###

git clone https://github.com/PRIDE-Archive/pride-mongodb-repo
mvn clean install -f pride-mongodb-repo/pom.xml -P mongodb-pridedb-pro-machines,mongodb-pridedb-prod-user-rw,oracle-pridedb-ro-user

###### Build Solr #####

git clone https://github.com/PRIDE-Archive/pride-solr-indexes
mvn clean install -f pride-solr-indexes/pom.xml -P mongodb-pridedb-localhost-machines,oracle-pridedb-test-machine,oracle-pridedb-ro-user

##### Build PRIDE Utilities ####

git clone https://github.com/PRIDE-Utilities/pride-utilities
mvn clean install


###### Delete the tmp folder ########
cd ../
rm -rfv tmp/


