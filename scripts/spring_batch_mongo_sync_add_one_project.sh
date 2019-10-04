#!/bin/sh

#This job syncs one document(accession based) from oracle into mongodb

##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="spring_batch_mongo_sync_accession_$1"
# the job parameters that are going to be passed on to the job (build below) in this case the project accession id
JOB_PARAMETERS=$1
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

bsub -M ${MEMORY_LIMIT} -R \"rusage[mem=${MEMORY_LIMIT}]\" -q production-rh74 -u ${JOB_EMAIL} -J ${JOB_NAME} \
    java -jar ${JAR_FILE_PATH}/revised-archive-submission-pipeline.jar --spring.batch.job.names=syncOracleToMongoProjectsJob \
    -Dspring-boot.run.arguments= --accession=${JOB_PARAMETERS} > ${LOG_PATH}/${LOG_FILE} 2>&1
