#!/bin/sh

# Load environment (and make the bsub command available)
. /etc/profile.d/lsf.sh

# job details
JOB_NAME="pride-stats-data"
DATE=$(date +"%Y%m%d%H%M")
EMAIL="pride-report@ebi.ac.uk"
LOG_PATH="./log/${JOB_NAME}"
JAR_PATH="./revised-archive-submission-pipeline.jar"
JAVA_DIR="/nfs/pride/work/java/jdk1.8.0_144/bin/"

LOG_FILE="${JOB_NAME}-${DATE}.log"

mkdir -p ${LOG_PATH}

# submit the job to LSF
bsub -M 3000 -R "rusage[mem=3000]" -q research-rh74 -u ${EMAIL} -J ${JOB_NAME} \
    ${JAVA_DIR}java -Xmx2500m -jar ${JAR_PATH} --spring.batch.job.names=computeSubmissionStatsJob \
    > ${LOG_PATH}/${LOG_FILE} 2>&1

