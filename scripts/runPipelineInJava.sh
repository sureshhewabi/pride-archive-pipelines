#!/bin/sh
################################# Warning ##################################################
# This script shouldn't be called directly !!!
# This is used mainly to log the job output to separte file instead of LSF email directly

LOG_PATH=$1
LOG_FILE_NAME=$2
MEMORY_LIMIT=$3
shift 3
PIPELINE_JOB_PARAMETERS=$*
#JAVA_DIR="${java.bin.directory}"
JAVA_DIR=""

mkdir -p ${LOG_PATH}

${JAVA_DIR}java -Xmx${MEMORY_LIMIT} ${PIPELINE_JOB_PARAMETERS} > ${LOG_PATH}/${LOG_FILE_NAME} 2>&1
