#!/usr/bin/env bash

# Load environment (and make the bsub command available)
. /etc/profile.d/lsf.sh

##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="sync_missing_px_projects"
# memory limit
MEMORY_LIMIT=6000
# memory overhead
MEMORY_OVERHEAD=1000
# LSF email notification
JOB_EMAIL="pride-report@ebi.ac.uk"
# Log file path
LOG_PATH="./log/${JOB_NAME}/"
# Log file name
LOG_FILE_NAME=""

##### FUNCTIONS
printUsage() {
    echo "Description: In the revised archive pipeline, this will sync all the missing projects with ProteomeXchange"
    echo "$ ./scripts/runSyncMissingPXProjects.sh"
    echo ""
    echo "Usage: ./runSyncMissingPXProjects.sh"
}

##### Set variables
DATE=$(date +"%Y%m%d%H%M")
LOG_FILE_NAME="${JOB_NAME}-${DATE}.log"
MEMORY_LIMIT_JAVA=$((MEMORY_LIMIT-MEMORY_OVERHEAD))

##### Change directory to where the script locate
cd ${0%/*}

#### RUN it on the production queue #####
bsub -M ${MEMORY_LIMIT} \
     -R \"rusage[mem=${MEMORY_LIMIT}]\" \
     -q production-rh74 \
     -g /pride/analyze_assays \
     -u ${JOB_EMAIL} \
     -J ${JOB_NAME} \
     ./runPipelineInJava.sh ${LOG_PATH} ${LOG_FILE_NAME} ${MEMORY_LIMIT_JAVA}m -jar revised-archive-submission-pipeline.jar --spring.batch.job.names=syncMissingProjectsOracleToPXJob