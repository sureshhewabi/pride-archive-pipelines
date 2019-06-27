#!/usr/bin/env bash

#This job syncs all documents from oracle into mongodb

##### OPTIONS
# (required) the project accession
ACCESSION=""

# (required) the assay accession
ASSAY_ACCESSION=""

##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="assay_analise"
# memory limit
MEMORY_LIMIT=6000
# memory overhead
MEMORY_OVERHEAD=1000
# LSF email notification
JOB_EMAIL="pride-report@ebi.ac.uk"
#JOB_EMAIL=${pride.report.email}
# Log file name
DATE=$(date +"%Y%m%d")
LOG_PATH=/nfs/pride/work/archive/revised-archive-submission-scripts/log
OUT_LOG_FILE_NAME=${JOB_NAME}-${DATE}"_out.log"
ERR_LOG_FILE_NAME=${JOB_NAME}-${DATE}"_err.log"
#JAR FILE PATH
JAR_FILE_PATH=/nfs/pride/work/archive/revised-archive-submission-pipeline

##### FUNCTIONS
printUsage() {
    echo "Description: In the revised archive pipeline, this will analyse the assay and calculate the assay statistics"
    echo "$ ./scripts/runAssayAnalyse.sh"
    echo ""
    echo "Usage: ./runAssayAnalyse.sh -a|--accession -s|--assay_accession [-e|--email]"
    echo "     Example: ./runAssayAnalyse.sh -a PXD011181 -s 99258"
    echo "     (required) accession         : the project accession"
    echo "     (required) assay_accession   : the assay accession"
    echo "     (optional) email             :  Email to send LSF notification"
}

##### PARSE the provided parameters
while [ "$1" != "" ]; do
    case $1 in
      "-a" | "--accession")
        shift
        ACCESSION=$1
        LOG_FILE_NAME="${ACCESSION}-${JOB_NAME}"
        MEMORY_LIMIT_JAVA=$((MEMORY_LIMIT-MEMORY_OVERHEAD))
        ;;
      "-s" | "--assay_accession")
        shift
        ASSAY_ACCESSION=$1
        ;;
    esac
    shift
done

JOB_NAME="${JOB_NAME}-${ACCESSION}-${ASSAY_ACCESSION}"
JOB_PARAMETERS="--accession=${ACCESSION},--assay_accession=${ASSAY_ACCESSION}"

##### CHECK the provided arguments
if [ -z ${ACCESSION} ]; then
         echo "Need to enter a project accession"
         printUsage
         exit 1
fi
if [ -z ${ASSAY_ACCESSION} ]; then
         echo "Need to enter a assay accession"
         printUsage
         exit 1
fi

#$LOG_FILE_TEMP=/nfs/pride/work/archive/revised-archive-submission-scripts/temp.txt
#echo "$(date "+%FT%T") $ACCESSION $ASSAY_ACCESSION" >> "$LOG_FILE_TEMP"

#### RUN it on the production queue #####
bsub -M ${MEMORY_LIMIT} \
     -R "rusage[mem=${MEMORY_LIMIT}]" \
     -q production-rh7 \
     -g /pride_assay_analise \
     -u ${JOB_EMAIL} \
     -J ${JOB_NAME} \
     -o ${LOG_PATH}/assay_analyse/${OUT_LOG_FILE_NAME} \
     -e ${LOG_PATH}/assay_analyse/${ERR_LOG_FILE_NAME} \
     java -jar ${JAR_FILE_PATH}/revised-archive-submission-pipeline.jar --spring.batch.job.names=PrideAnalysisAssayInformation -Dspring-boot.run.arguments=${JOB_PARAMETERS}