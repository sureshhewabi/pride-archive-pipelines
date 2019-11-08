#!/bin/sh
################################# Warning ##################################################
# This script shouldn't be called directly !!!
# This is used mainly to log the job output to separte file instead of LSF email directly

LOG_PATH=$1
LOG_FILE_NAME=$2
MEMORY_LIMIT=$3
shift 3
PIPELINE_JOB_PARAMETERS=$*

JAVA_DIR="/nfs/pride/work/java/jdk1.8.0_65/bin/"

mkdir -p ${LOG_PATH}

${JAVA_DIR}java -Xmx${MEMORY_LIMIT} ${PIPELINE_JOB_PARAMETERS} > ${LOG_PATH}/${LOG_FILE_NAME} 2>&1

CODE=$?

while [ "$#" -gt 0 ]; do
  case "$1" in
    --spring.batch.job.names=*) job_name="${1#*=}"; shift 1;;
    *) shift 1;;
  esac
done

if [ $CODE -eq 0 ]
then
     MSG="\`${job_name}\` pipeline finished \`SUCCESSFULLY\`"
else
     MSG="ERROR: \`${job_name}\` pipeline \`FAILED\` with code:  ${CODE}"
fi

MSG="${MSG}\nLOG: \`${LOG_PATH}/${LOG_FILE_NAME}\`"

curl -X POST --data-urlencode "payload={ \"text\": \"$MSG\"}" $SLACK_REPORT_URL || true