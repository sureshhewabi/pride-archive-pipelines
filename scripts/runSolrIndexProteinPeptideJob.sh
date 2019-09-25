#!/usr/bin/env bash

#This job import assay information into mongodb

##### OPTIONS
# (required) the project accession
PROJECT_ACCESSION=""

##### VARIABLES
# the name to give to the LSF job (to be extended with additional info)
JOB_NAME="solr_index_peptide_protein"
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

#JAR FILE PATH
JAR_FILE_PATH=/nfs/pride/work/archive/revised-archive-submission-pipeline

##### FUNCTIONS
printUsage() {
    echo "Description: In the revised archive pipeline, This job indexes peptides & proteins to Solr from MongoDB"
    echo "$ ./scripts/runSolrIndexProteinPeptideJob.sh"
    echo ""
    echo "Usage: ./runSolrIndexProteinPeptideJob.sh -a|--accession [-e|--email]"
    echo "     Example: ./runSolrIndexProteinPeptideJob.sh -a PXD011181 -s 99258"
    echo "     (required) accession         : the project accession"
    echo "     (optional) email             :  Email to send LSF notification"
}

JOB_ARGS=""

##### PARSE the provided parameters
while [ "$1" != "" ]; do
    case $1 in
      "-a" | "--accession")
        shift
        PROJECT_ACCESSION=$1
        JOB_ARGS="${JOB_ARGS} project=${PROJECT_ACCESSION}"
        ;;
    esac
    shift
done

JOB_NAME="${JOB_NAME}-${PROJECT_ACCESSION}"

##### CHECK the provided arguments
if [ -z ${PROJECT_ACCESSION} ]; then
         echo "Running for all projects as no project accession has been provided"
fi

#echo ${JOB_ARGS}

#### RUN it on the production queue #####
bsub -M ${MEMORY_LIMIT} \
     -R "rusage[mem=${MEMORY_LIMIT}]" \
     -q research-rh74 \
     -g /pride/analyze_assays \
     -u ${JOB_EMAIL} \
     -J ${JOB_NAME} \
     java -jar ${JAR_FILE_PATH}/revised-archive-submission-pipeline.jar --spring.batch.job.names=solrIndexPeptideProteinJob ${JOB_ARGS} > ${LOG_PATH}/import_assay/${OUT_LOG_FILE_NAME} 2>&1

