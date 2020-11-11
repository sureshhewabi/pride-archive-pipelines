#!/usr/bin/env bash

# Load environment (and make the bsub command available)
. /etc/profile.d/lsf.sh

##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="spring_batch_save_sdrf_github_to_biosamples_and_mongo"
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
    echo "Description: In the revised archive pipeline, this will save the sdrf to bio samples and mongo"
    echo "$ ./scripts/spring_batch_save_sdrf_github_to_biosamples_and_mongo.sh.sh"
    echo ""
    echo "Usage: ./spring_batch_save_sdrf_github_to_biosamples_and_mongo.sh.sh -f|--folderPath [-e|--email]"
    echo "     Example: ./spring_batch_save_sdrf_github_to_biosamples_and_mongo.sh.sh -f /nfs/release/pride/project/annotated/PXD000001/"
    echo "     (required) accession         : the project accession"
    echo "     (optional) email             :  Email to send LSF notification"
}

##### PARSE the provided parameters
while [ "$1" != "" ]; do
    case $1 in
      "-f" | "--folderPath")
        shift
        sdrf_folder_path=$1
        ;;
    esac
    shift
done

##### CHECK the provided arguments
if [ -z ${sdrf_folder_path} ]; then
         echo "Need to enter the folder path of sdrf"
         printUsage
         exit 1
fi

##### Set variables
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
     ./runPipelineInJava.sh ${LOG_PATH} ${LOG_FILE_NAME} ${MEMORY_LIMIT_JAVA}m -jar revised-archive-submission-pipeline.jar --spring.batch.job.names=sdrfSaveToBioSamplesAndMongoJob -Dspring-boot.run.arguments= --projectsSdrfFolder=${sdrf_folder_path}
