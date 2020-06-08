#!/bin/sh
# Load environment (and make the bsub command available)
. /etc/profile.d/lsf.sh

##### OPTIONS
# (required either: ) the project accession if available (required if a project accession has already been assigned to the datatset)
ACCESSION=""
# (required or: ) all projects to process
ALL=""
# (optional)  a time stamp in the format: yyy-mm-dd
DATE=$(date +"%Y%m%d%H%M")

##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="priderEbeyeXmlGenerationJob"
# the job parameters that are going to be passed on to the job (build below)
JOB_PARAMETERS="random.number="$RANDOM
# memory limit
MEMORY_LIMIT=6000
# memory overhead
MEMORY_OVERHEAD=1000
# LSF email notification
JOB_EMAIL="pride-report@ebi.ac.uk"
# Log file name
LOG_FILE_NAME=""
# Log file path
LOG_PATH="./log/${JOB_NAME}/"

##### FUNCTIONS
printUsage() {
    echo "Description: For generation of search parameters to output tab-spaced text file for 'complete' projects."
    echo ""
    echo "Usage: ./runEBeyeXMLGeneration.sh -a|--accession -all [-d|--date]"
    echo "     Example: ./runEBeyeXMLGeneration.sh -a PXD000542"
    echo "     Example: ./runEBeyeXMLGeneration.sh -all"
    echo "     (required either:) accession: the project accession"
    echo "     (required or: ) accession: the project accession"
    echo "     (optional) date   : the time stamp date in the format: yyyy-mm-dd"
}


DATE=$(date +"%Y%m%d%H%M")
LOG_FILE_NAME="${JOB_NAME}-${DATE}.log"
MEMORY_LIMIT_JAVA=$((MEMORY_LIMIT-MEMORY_OVERHEAD))

##### PARSE the provided parameters
while [ "$1" != "" ]; do
    case $1 in
      "-a" | "--accession")
        shift
        ACCESSION=$1
        LOG_FILE_NAME="${ACCESSION}-${JOB_NAME}-${DATE}.log"
        JOB_NAME="${JOB_NAME}-${ACCESSION}"
        JOB_PARAMETERS="${JOB_PARAMETERS} --project.accession=${ACCESSION}"
        ;;
      "-all")
        shift
        ALL=true
        LOG_FILE_NAME="ebeye-allpub-${JOB_NAME}-${DATE}.log"
        JOB_NAME="${JOB_NAME}-ebeye-allpub"
        JOB_PARAMETERS="${JOB_PARAMETERS} --process.all=${ALL}"
        ;;
      "-e" | "--email")
        shift
        JOB_EMAIL=$1
        ;;
      "-d" | "--date")
        shift
        DATE=$1
        JOB_PARAMETERS="${JOB_PARAMETERS} --time.stamp=${DATE}"
        ;;
    esac
    shift
done


##### CHECK the provided arguments
if [ -z ${ACCESSION} ]  && [ -z ${ALL} ]; then
         echo "Need to enter a project accession or set all to true"
         printUsage
         exit 1
fi

##### Change directory to where the script locate
cd ${0%/*}

##### RUN it on the production queue #####
##### NOTE: you can change LSF group to modify the number of jobs can be run concurrently #####
bsub -M ${MEMORY_LIMIT} \
     -R "rusage[mem=${MEMORY_LIMIT}]" \
     -q production-rh74 \
     -g /pride_ebye_xml_gen \
     -u ${JOB_EMAIL} \
     -J ${JOB_NAME} \
     ./runPipelineInJava.sh ${LOG_PATH} ${LOG_FILE_NAME} ${MEMORY_LIMIT_JAVA}m -jar revised-archive-submission-pipeline.jar --spring.batch.job.names=priderEbeyeXmlGenerationJob ${JOB_PARAMETERS}

