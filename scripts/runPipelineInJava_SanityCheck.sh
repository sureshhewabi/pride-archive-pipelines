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

${JAVA_DIR}java -Xmx${MEMORY_LIMIT} ${PIPELINE_JOB_PARAMETERS} > ${LOG_FILE_LOCAL} 2>&1

CODE=$?

while [ "$#" -gt 0 ]; do
  case "$1" in
    --spring.batch.job.names=*) job_name="${1#*=}"; shift 1;;
    *) shift 1;;
  esac
done

slack_url=$SLACK_REPORT_URL

if [ $CODE -eq 0 ]
then
     MSG="*${job_name}* pipeline finished _SUCCESSFULLY_"
else
     MSG="*ERROR*: \`${job_name}\` pipeline \`FAILED\` with code:  ${CODE}"
     slack_url=$SLACK_ERROR_REPORT_URL
fi

MSG="${MSG} \n (${PIPELINE_JOB_PARAMETERS}) \n LOG: _${LOG_FILE_FULL_PATH}_ \n ------------------------------------------------------------------------------ "

curl -X POST --data-urlencode "payload={ \"text\": \"$MSG\"}" $slack_url || true

if [ $CODE -eq 0 ]
then
  report_file="Sanity_check_report_`date '+%Y_%m_%d'`.txt"
  grep ERROR ${LOG_FILE_FULL_PATH} | grep "====" | awk -F'====' '{print $2}' > ${report_file}
  curl -F file=@${report_file} -F "initial_comment=Errors found by sanity check pipeline: ${report_file}" -F channels=${sanity_check_report_slack_channel} -H "Authorization: Bearer ${SLACK_APP_TOKEN}" https://slack.com/api/files.upload
  rm ${report_file}
fi

exit $CODE # so that LSF reports correct status in EMAIL