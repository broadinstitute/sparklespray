import subprocess
import os
from sparklespray.util import random_string

services_to_add = [ #"storage.googleapis.com", 
    "datastore.googleapis.com", "storage-component.googleapis.com", "genomics.googleapis.com", "pubsub.googleapis.com", "storage-api.googleapis.com", "compute.googleapis.com"]

roles_to_add = ["roles/compute.admin",
    "roles/datastore.user", 
    "roles/genomics.pipelinesRunner",
    "roles/pubsub.editor",
    "roles/storage.objectAdmin"]

def gcloud(args):
    cmd = ['gcloud'] + args
    subprocess.check_call(cmd)

def enable_services(project_id):
    for service in services_to_add:
        gcloud(['service-management','enable', service, '--project', project_id])

def create_service_account(service_acct, project_id, key_path):
    gcloud(['iam', 'service-accounts', 'create', service_acct, '--project', project_id, '--display-name', "Service account for sparklespray"])

    for role in roles_to_add:
        gcloud(['projects', 'add-iam-policy-binding', project_id, '--member', f"serviceAccount:{service_acct}@{project_id}.iam.gserviceaccount.com", "--role", role])

    gcloud(['iam', 'service-accounts', 'keys', 'create', key_path, '--iam-account', f"{service_acct}@{project_id}.iam.gserviceaccount.com"])

def setup_project(project_id, service_acct_cache_path):
    #enable_services(project_id)
    service_acct = "sparkles-"+random_string(8).lower()
    key_path = os.path.join(service_acct_cache_path, project_id+".json")
    if not os.path.exists(key_path):
        parent = os.path.dirname(key_path)
        if not os.path.exists(parent):
            os.makedirs(parent)
        create_service_account(service_acct, project_id, key_path)

setup_project("broad-achilles", "./service-keys")
