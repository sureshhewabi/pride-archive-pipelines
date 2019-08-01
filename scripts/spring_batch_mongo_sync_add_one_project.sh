#!/bin/sh

#This job syncs one document(accession based) from oracle into mongodb

##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="spring_batch_mongo_sync_accession"
# the job parameters that are going to be passed on to the job (build below) in this case the project accession id
JOB_PARAMETERS=$1
# memory limit
MEMORY_LIMIT=4096
# LSF email notification
JOB_EMAIL="pride-report@ebi.ac.uk"
# Log file name
DATE=$(date +"%Y%m%d")
LOG_PATH=/nfs/pride/work/archive/revised-archive-submission-scripts/log
OUT_LOG_FILE_NAME=${JOB_NAME}-${DATE}"_out.log"
ERR_LOG_FILE_NAME=${JOB_NAME}-${DATE}"_err.log"
#JAR FILE PATH
JAR_FILE_PATH=/nfs/pride/work/archive/revised-archive-submission-pipeline

bsub -M ${MEMORY_LIMIT} -R \"rusage[mem=${MEMORY_LIMIT}]\" -q production-rh7 -u ${JOB_EMAIL} -J ${JOB_NAME} -o ${LOG_PATH}/mongosync/${OUT_LOG_FILE_NAME} -e ${LOG_PATH}/mongosync/${ERR_LOG_FILE_NAME} java -jar ${JAR_FILE_PATH}/revised-archive-submission-pipeline.jar --spring.batch.job.names=PrideArchiveOracleSyncToMongoDB -Dspring-boot.run.arguments= --accession=${JOB_PARAMETERS}
