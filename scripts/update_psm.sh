#!/usr/bin/env bash

# Load environment (and make the bsub command available)
. /etc/profile.d/lsf.sh

#This job syncs one document(accession based) from oracle into mongodb


##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="update_psm"
# memory limit
MEMORY_LIMIT=6000
# memory overhead
MEMORY_OVERHEAD=1000
# java memory limit
MEMORY_LIMIT_JAVA=0
# LSF email notification
JOB_EMAIL="pride-report@ebi.ac.uk"
# Log file path
LOG_PATH="./log/${JOB_NAME}/"
# Log file name
LOG_FILE_NAME=""

##### FUNCTIONS
printUsage() {
    echo "Description: In the revised archive pipeline, this will import one project information to mongoDB"
    echo "$ ./scripts/update_psm.sh"
    echo ""
    echo "Usage: ./update_psm.sh"
}
JOB_ARGS=""
##### PARSE the provided parameters
while [ "$1" != "" ]; do
    case $1 in
      "-a" | "--accession")
        shift
        PROJECT_ACCESSION=$1
        JOB_ARGS="${JOB_ARGS} --accession=${PROJECT_ACCESSION}"
        ;;
    esac
    shift
done

##### Set variables
JOB_NAME="${JOB_NAME}"
DATE=$(date +"%Y%m%d%H%M")
LOG_FILE_NAME="${JOB_NAME}-${DATE}.log"
MEMORY_LIMIT_JAVA=$((MEMORY_LIMIT-MEMORY_OVERHEAD))

##### Change directory to where the script locate
cd ${0%/*}

#### RUN it on the production queue #####
bsub -M ${MEMORY_LIMIT} \
     -R "rusage[mem=${MEMORY_LIMIT}]" \
     -q production-rh74 \
     -g /pride/analyze_assays \
     -u ${JOB_EMAIL} \
     -J ${JOB_NAME} \
     ./runPipelineInJava.sh ${LOG_PATH} ${LOG_FILE_NAME} ${MEMORY_LIMIT_JAVA}m -jar revised-archive-submission-pipeline.jar --spring.datasource.maxPoolSize=10 --spring.batch.job.names=mongoUpdatePsmJobJobBean -Dspring-boot.run.arguments=${JOB_ARGS}
