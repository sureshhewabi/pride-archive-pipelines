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

LOG_FILE_LOCAL=${LOG_PATH}/${LOG_FILE_NAME}

if [[ ${LOG_PATH} == ./* ]]; then
    curdir="`readlink -f \`pwd\``/"
fi

LOG_FILE_FULL_PATH="${curdir}${LOG_FILE_LOCAL}"

## ---- filebeat start ---
#export log_file=${LOG_FILE_FULL_PATH}
#nohup ${FILE_BEAT_PATH}/filebeat -c ${FILE_BEAT_PATH}/filebeat.yml > /dev/null 2>&1 &
#filebeat_pid=$!
## ---- filebeat end ---

${JAVA_DIR}java -Xmx${MEMORY_LIMIT} ${PIPELINE_JOB_PARAMETERS} > ${LOG_FILE_LOCAL} 2>&1

CODE=$?

while [ "$#" -gt 0 ]; do
  case "$1" in
    --spring.batch.job.names=*) job_name="${1#*=}"; shift 1;;
    *) shift 1;;
  esac
done

if [ $CODE -eq 0 ]
then
     MSG="*${job_name}* pipeline finished _SUCCESSFULLY_"
else
     MSG="*ERROR*: \`${job_name}\` pipeline \`FAILED\` with code:  ${CODE}"
fi

MSG="${MSG} \n (${PIPELINE_JOB_PARAMETERS}) \n LOG: _${LOG_FILE_FULL_PATH}_ \n ------------------------------------------------------------------------------ "

curl -X POST --data-urlencode "payload={ \"text\": \"$MSG\"}" $SLACK_REPORT_URL || true

#kill $filebeat_pid