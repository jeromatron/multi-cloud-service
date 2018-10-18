#!/bin/bash

usage="--------------------------------------------------------------------------
Deploys vms in GCP based on parameters in ./gcp/clusterParameters.yaml
using Google Deployment Manager (deployment-manager).

Usage:
deploy.sh [-h] [-d deployment-name]

Options:

 -h                 : display this message and exit
 -d		    : name of GCP gcloud deployment [required]

--------------------------------------------------------------------------"

while getopts 'hd:' opt; do
  case $opt in
    h) echo -e "$usage"
       exit 0
    ;;
    d) deploy="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
        exit 1
    ;;
  esac
done

echo "Deploying 'clusterParameters.yaml' in GCP gcloud deployment: $deploy"
gcloud deployment-manager deployments create $deploy --config ./gcp/clusterParameters.yaml --labels delpoyer-app=assethub,create_user=sebastian_estevez_datastax_com,org=presales
