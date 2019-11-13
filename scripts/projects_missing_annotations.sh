DATE=$(date +"%Y%m%d_%H%M")
EXPORT_JSON_FILE_NAME="pride_projects-${DATE}.json"
EXPORT_CSV_FILE_NAME="pride_projects-${DATE}.csv"
GITHUB_CSV_FILE_NAME="github_file.csv"

EXPORT_JSON_FILE=$EXPORT_PATH/${EXPORT_JSON_FILE_NAME}
EXPORT_CSV_FILE=$EXPORT_PATH/${EXPORT_CSV_FILE_NAME}
GITHUB_CSV_FILE=$EXPORT_PATH/${GITHUB_CSV_FILE_NAME}

mkdir -p $EXPORT_PATH

${MONGO_BIN_PATH}/mongoexport --uri="$mongodb_machine_uri" --collection=pride_projects --out="${EXPORT_JSON_FILE}" --fields='accession,instruments,quantificationMethods,softwareList,ptmList,sample_attributes,project_references'

wget https://raw.githubusercontent.com/PRIDE-Utilities/pride-ontology/master/pride-annotations/pride_projects.csv  -O ${GITHUB_CSV_FILE}

${NODE_PATH}/node missing_annotations.js ${EXPORT_JSON_FILE} ${EXPORT_CSV_FILE} ${GITHUB_CSV_FILE} "$@"

curl -F file=@${EXPORT_CSV_FILE} -F "initial_comment=Missing annotations: ${EXPORT_CSV_FILE_NAME}" -F channels=review -H "Authorization: Bearer ${SLACK_APP_TOKEN}" https://slack.com/api/files.upload

rm ${EXPORT_JSON_FILE} ${EXPORT_CSV_FILE} ${GITHUB_CSV_FILE}