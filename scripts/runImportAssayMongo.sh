#!/usr/bin/env bash

#This job import assay information into mongodb

##### OPTIONS
# (required) the project accession
PROJECT_ACCESSION=""

##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="import_assay_mongo"
# memory limit
MEMORY_LIMIT=6000
# memory overhead
MEMORY_OVERHEAD=1000
# LSF email notification
JOB_EMAIL="pride-report@ebi.ac.uk"
# Log file name
DATE=$(date +"%Y%m%d")
LOG_PATH=/nfs/pride/work/archive/revised-archive-submission-scripts/log
OUT_LOG_FILE_NAME=${JOB_NAME}-${DATE}"_out.log"
ERR_LOG_FILE_NAME=${JOB_NAME}-${DATE}"_err.log"
#JAR FILE PATH
JAR_FILE_PATH=/nfs/pride/work/archive/revised-archive-submission-pipeline

##### FUNCTIONS
printUsage() {
    echo "Description: In the revised archive pipeline, this will import one or multiple assay information to mongoDB"
    echo "$ ./scripts/runImportAssayMongo.sh"
    echo ""
    echo "Usage: ./runImportAssayMongo.sh -a|--accession [-e|--email]"
    echo "     Example: ./runAssayAnalyse.sh -a PXD011181 -s 99258"
    echo "     (required) accession         : the project accession"
    echo "     (optional) email             :  Email to send LSF notification"
}

##### PARSE the provided parameters
while [ "$1" != "" ]; do
    case $1 in
      "-a" | "--accession")
        shift
        PROJECT_ACCESSION=$1
        LOG_FILE_NAME="${PROJECT_ACCESSION}-${JOB_NAME}"
        MEMORY_LIMIT_JAVA=$((MEMORY_LIMIT-MEMORY_OVERHEAD))
        ;;
    esac
    shift
done

JOB_NAME="${JOB_NAME}-${PROJECT_ACCESSION}"

##### CHECK the provided arguments
if [ -z ${PROJECT_ACCESSION} ]; then
         echo "Need to enter a project accession"
         printUsage
         exit 1
fi

#### RUN it on the production queue #####
bsub -M ${MEMORY_LIMIT} \
     -R "rusage[mem=${MEMORY_LIMIT}]" \
     -q production-rh7 \
     -g /pride/analyze_assays \
     -u ${JOB_EMAIL} \
     -J ${JOB_NAME} \
     -o ${LOG_PATH}/assay_analyse/${OUT_LOG_FILE_NAME} \
     -e ${LOG_PATH}/assay_analyse/${ERR_LOG_FILE_NAME} \
     java -jar ${JAR_FILE_PATH}/revised-archive-submission-pipeline.jar --spring.batch.job.names=importProjectAssaysInformationJob project=${PROJECT_ACCESSION}
