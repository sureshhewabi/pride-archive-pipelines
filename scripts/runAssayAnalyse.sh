#!/usr/bin/env bash

##### OPTIONS
# (required) the project accession
PROJECT_ACCESSION=""

# (required) the assay accession
ASSAY_ACCESSION=""

##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="assay_analyse"
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
        PROJECT_ACCESSION=$1
        ;;
      "-s" | "--assay_accession")
        shift
        ASSAY_ACCESSION=$1
        ;;
    esac
    shift
done

##### CHECK the provided arguments
if [ -z ${PROJECT_ACCESSION} ]; then
         echo "Need to enter a project accession"
         printUsage
         exit 1
fi
if [ -z ${ASSAY_ACCESSION} ]; then
         echo "Need to enter a assay accession"
         printUsage
         exit 1
fi

##### Set variables
JOB_NAME="${JOB_NAME}-${PROJECT_ACCESSION}-${ASSAY_ACCESSION}"
DATE=$(date +"%Y%m%d%H%M")
LOG_FILE_NAME="${JOB_NAME}-${DATE}.log"
MEMORY_LIMIT_JAVA=$((MEMORY_LIMIT-MEMORY_OVERHEAD))

##### Change directory to where the script locate
cd ${0%/*}

#### RUN it on the production queue #####
bsub -M ${MEMORY_LIMIT} \
     -R \"rusage[mem=${MEMORY_LIMIT}]\" \
     -q research-rh74 \
     -g /pride/analyze_assays \
     -u ${JOB_EMAIL} \
     -J ${JOB_NAME} \
     ./runPipelineInJava.sh ${LOG_PATH} ${LOG_FILE_NAME} ${MEMORY_LIMIT_JAVA}m -jar revised-archive-submission-pipeline.jar --spring.batch.job.names=analyzeAssayInformationJob project=${PROJECT_ACCESSION} assay=${ASSAY_ACCESSION}