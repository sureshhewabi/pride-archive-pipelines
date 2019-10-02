#!/bin/sh

#This job syncs all documents from oracle into mongodb

##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="spring_batch_solr_sync"
# memory limit
MEMORY_LIMIT=4096
# LSF email notification
JOB_EMAIL="pride-report@ebi.ac.uk"
# Log file name
DATE=$(date +"%Y%m%d")
LOG_PATH=./log
OUT_LOG_FILE_NAME=${JOB_NAME}-${DATE}"_out.log"
ERR_LOG_FILE_NAME=${JOB_NAME}-${DATE}"_err.log"
#JAR FILE PATH
JAR_FILE_PATH=.

bsub -M ${MEMORY_LIMIT} -R \"rusage[mem=${MEMORY_LIMIT}]\" -q research-rh74 -u ${JOB_EMAIL} -J ${JOB_NAME} -o ${LOG_PATH}/solrsync/${OUT_LOG_FILE_NAME} -e ${LOG_PATH}/solrsync/${ERR_LOG_FILE_NAME} java -jar ${JAR_FILE_PATH}/revised-archive-submission-pipeline.jar --spring.batch.job.names=syncMongoProjectToSolrCloudJob

