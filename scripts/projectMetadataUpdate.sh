#!/usr/bin/env bash

# Load environment (and make the bsub command available)
. /etc/profile.d/lsf.sh

##### OPTIONS
# (required) the project accession
PROJECT_ACCESSION=""

# (required) the assay accession
SUMMERY_FILE_ABS_PATH=""

##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="project_metadata_update"
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
    echo "Description: In the revised archive pipeline, this will update the project metadata"
    echo "$ ./scripts/projectMetadataUpdate.sh"
    echo ""
    echo "Usage: ./projectMetadataUpdate.sh -a|--accession -f|--abs_file_path [-e|--email]"
    echo "     Example: ./projectMetadataUpdate.sh -a PXD010142 -f /nfs/pride/prod/archive/2018/09/PXD010142/tmp/PXD010142.px"
    echo "     (required) accession         : The project accession"
    echo "     (required) abs_file_path     : Absolute file path for the modified submission.px file"
    echo "     (optional) email             : Email to send LSF notification"
}

##### PARSE the provided parameters
while [ "$1" != "" ]; do
    case $1 in
      "-a" | "--accession")
        shift
        PROJECT_ACCESSION=$1
        ;;
      "-f" | "--abs_file_path")
        shift
        SUMMERY_FILE_ABS_PATH=$1
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
if [ -z ${SUMMERY_FILE_ABS_PATH} ]; then
         echo "Need to enter absolute file path for the modified submission.px file"
         printUsage
         exit 1
fi

##### Set variables
JOB_NAME="${JOB_NAME}-${PROJECT_ACCESSION}"
DATE=$(date +"%Y%m%d%H%M")
LOG_FILE_NAME="${JOB_NAME}-${DATE}.log"
MEMORY_LIMIT_JAVA=$((MEMORY_LIMIT-MEMORY_OVERHEAD))

##### Change directory to where the script locate
cd ${0%/*}

#### RUN it on the production queue #####
bsub -M ${MEMORY_LIMIT} \
     -R \"rusage[mem=${MEMORY_LIMIT}]\" \
     -q research-rh74 \
     -g /pride/curation_tool \
     -u ${JOB_EMAIL} \
     -J ${JOB_NAME} \
     ./runPipelineInJava.sh ${LOG_PATH} ${LOG_FILE_NAME} ${MEMORY_LIMIT_JAVA}m -jar revised-archive-submission-pipeline.jar --spring.batch.job.names=projectMetadataUpdateJob projectAccession=${PROJECT_ACCESSION} modifiedSubmissionSummaryFile=${SUMMERY_FILE_ABS_PATH}