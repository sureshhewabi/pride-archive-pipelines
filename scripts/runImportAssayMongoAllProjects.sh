#!/usr/bin/env bash

#This job import all the assay information into mongodb

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
#JAR FILE PATH
JAR_FILE_PATH=.

##### FUNCTIONS
printUsage() {
    echo "Description: In the revised archive pipeline, this will import all assay information to mongoDB"
    echo "$ ./scripts/runImportAssayMongoAllProjects.sh"
    echo ""
    echo "Usage: ./runImportAssayMongoAllProjects.sh [-e|--email]"
    echo "     Example: ./runAssayAnalyse.sh"
    echo "     (required) accession         : the project accession"
    echo "     (optional) email             :  Email to send LSF notification"
}

LOG_FILE_NAME="${DATE}-${JOB_NAME}"
MEMORY_LIMIT_JAVA=$((MEMORY_LIMIT-MEMORY_OVERHEAD))
JOB_NAME="${JOB_NAME}"


#### RUN it on the production queue #####
bsub -M ${MEMORY_LIMIT} \
     -R "rusage[mem=${MEMORY_LIMIT}]" \
     -q research-rh74 \
     -g /pride/analyze_assays \
     -u ${JOB_EMAIL} \
     -J ${JOB_NAME} \
     java -jar ${JAR_FILE_PATH}/revised-archive-submission-pipeline.jar --spring.batch.job.names=importProjectAssaysInformationJob
