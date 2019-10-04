#!/bin/sh

#This job syncs all documents from oracle into mongodb

##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="spring_batch_mongo_sync"
# memory limit
MEMORY_LIMIT=4096
# LSF email notification
JOB_EMAIL="pride-report@ebi.ac.uk"
# Log file name
DATE=$(date +"%Y%m%d")
LOG_PATH="./log/${JOB_NAME}"
LOG_FILE="${JOB_NAME}-${DATE}.log"
#JAR FILE PATH
JAR_FILE_PATH=.

mkdir -p ${LOG_PATH}

bsub -M ${MEMORY_LIMIT} -R \"rusage[mem=${MEMORY_LIMIT}]\" -q research-rh74 -u ${JOB_EMAIL} -J ${JOB_NAME} \
    java -jar ${JAR_FILE_PATH}/revised-archive-submission-pipeline.jar --spring.batch.job.names=syncOracleToMongoProjectsJob \
    > ${LOG_PATH}/${LOG_FILE} 2>&1
