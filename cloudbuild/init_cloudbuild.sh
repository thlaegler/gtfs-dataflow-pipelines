#!/bin/bash


usage() {
    cat <<EOF
NAME
    ${0##*/} Initialize GCP Cloud Build

SYNOPSIS
    ${0##*/} SERVICE_NAME PROJECT_ID BRANCH_NAME SHORT_SHA

Options:

    For example:
        ./init_cloudbuild.sh

RETURN CODES
    Returns 0 on success, 1 if an error occurs.

SEE ALSO
    See the documentation on Confluence for more details, including
    instructions on creating environments.

EOF
}

function initCloudbuild() {
    if [ -f "cloudbuild/$PROJECT_ID-cloudbuild.properties" ]; then
		cp cloudbuild/$PROJECT_ID-cloudbuild.properties cloudbuild.properties
		while read p; do
			echo "$(echo $p | cut -d '=' -f 2-)" > "$(echo $p | cut -d '=' -f1 )"
		done < cloudbuild.properties
		cat cloudbuild.properties
	fi
	if [ -f "cloudbuild/$PROJECT_ID-secret.properties.enc" ]; then
		gcloud kms decrypt --plaintext-file=secret.properties --ciphertext-file=cloudbuild/$PROJECT_ID-secret.properties.enc --location=global --keyring=$PROJECT_ID-global-keyring --key=$PROJECT_ID-global-key --project=$PROJECT_ID
		while read p; do
			echo "$(echo $p | cut -d '=' -f 2-)" > "$(echo $p | cut -d '=' -f1 )"
		done < secret.properties
		cat secret.properties
	fi 
	if [ -f "cloudbuild/$PROJECT_ID-service-account.json.enc" ]; then
		gcloud kms decrypt --plaintext-file=service-account.json --ciphertext-file=cloudbuild/$PROJECT_ID-service-account.json.enc --location=global --keyring=$PROJECT_ID-global-keyring --key=$PROJECT_ID-global-key --project=$PROJECT_ID
	fi
}

if [[ "$#" -eq 0 ]]; then
	initCloudbuild $1 $2 $3 $4 $5 $6 $7 $8 $9
else
    usage
    exit 1
fi
