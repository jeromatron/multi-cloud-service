#e!/usr/bin/env python
# Example provisioning API usage script.  (C) DataStax, 2015.  All Rights Reserved
#
# Needs these OS environmental variables pre-defined: lcm_server, cassandra_default_password, opscenter_session (optional), dse_ver (optional), cluster_name (optional)
# command line parameter with node IP/DC in the following format:
# public_IP:private_IP:DC_name:node_number

import os
import sys
import requests
import json
import threading
import argparse
import subprocess
import webbrowser

datastax_version = "6.0.2"
vnodes = 1

# Configurable args
ap = argparse.ArgumentParser()
ap.add_argument("-lcm", "--LCM_server_ip", required=True,
	help="public IP address of the LCM server")
ap.add_argument("-k", "--ssh_key", required=True,
	help="private key to be used")
ap.add_argument("-n", "--cluster_name", required=True,
	help="name of the cluster that you want to create")
ap.add_argument("-s", "--server_list", required=True,
	help="list of servers to be added to the new cluster")
ap.add_argument("-u", "--user", required=True,
	help="username for the server")
ap.add_argument("-v", "--version", required=False,
	help="username for the server")
ap.add_argument("-t", "--tokens", required=False,
	help="username for the server")

args = vars(ap.parse_args())

server_ip = args["LCM_server_ip"]
ssh_key = args["ssh_key"]
cluster_name = args["cluster_name"]
server_list = args["server_list"]
username = args["user"]
datastax_version = args["version"]
vnodes = int(args["tokens"])

jvmOptionsParam = "jvm-options"
if datastax_version.startswith("6.8"):
    jvmOptionsParam = "jvm-server-options"

repo_user = os.environ.get('academy_user').strip()
repo_pass = os.environ.get('academy_pass').strip()
download_token = os.environ.get('academy_token').strip()

#SSH into the OpsCenter/LCM server, install the JDK, install OpsCenter
bashCommand = 'ssh -o StrictHostKeyChecking=no -i '+ ssh_key+ ' '+ username+'@'+server_ip+' \'sudo apt-get install -y python software-properties-common; \
sudo apt-add-repository -y ppa:webupd8team/java; \
sudo apt-get update; \
sudo apt-get install -y openjdk-8-jdk; \
echo "deb https://'+repo_user+':'+download_token+'@debian.datastax.com/enterprise \
stable main" | sudo tee -a /etc/apt/sources.list.d/datastax.sources.list; \
curl -L https://debian.datastax.com/debian/repo_key | sudo apt-key add - ; \
sudo apt-get update; sudo apt-get install opscenter; sudo service opscenterd start\' 2>/dev/null'

output = subprocess.check_output(['bash','-c', bashCommand])

base_url = 'http://%s:8888/api/v2/lcm/' % server_ip
cassandra_default_password = os.environ.get('cassandra_default_password').strip()
opscenter_session = os.environ.get('opscenter_session', '')

# Wait for OpsCenter to finish starting up by polling the API until we get a
# 20x response.

# Works by attaching a custom retry mechanism to the requests session, this is
# documented at:
# http://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html
# https://www.peterbe.com/plog/best-practice-with-retries-with-requests
#
# This particular session waits a max around 4 minutes.
# This affects only requests invoked via the custom-session, not requests
# invoked via requests.post or requests.get, for example.
#
# By default session.get(base_url) will silently block until OpsCenter comes up.
# This StackOverflow answer shows how to turn up the verbosity on third-party
# loggers (inluding requests and retry) and send them to stdout, but it will
# make the rest of script extremely verbose as well:
# https://stackoverflow.com/a/14058475
session = requests.Session()
retry = requests.packages.urllib3.util.retry.Retry(
    total=8,          # Max retry attempts
    backoff_factor=1, # Sleeps for [ 1s, 2s, 4s, ... ]
                      # Stops growing at 120 seconds
)
adapter = requests.adapters.HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)
session.get(base_url)

print str(sys.argv)

def do_post(url, post_data):
    result = requests.post(base_url + url,
                           data=json.dumps(post_data),
                           headers={'Content-Type': 'application/json', 'opscenter-session': opscenter_session})
    print repr(result.text)
    result_data = json.loads(result.text)
    return result_data

repository_response = do_post("repositories/",
    {"name": "dse-public-repo",
        "username": repo_user,
        "password": repo_pass,})

repository_id = repository_response['id']

# ssh private key example
with open(ssh_key, 'r') as myfile:
        privateKey=myfile.read()
machine_credential_response = do_post("machine_credentials/",
     {"name": "playground-multi-cloud",
      "login-user": username,
      "become-mode": "sudo",
      "ssh-private-key": privateKey,
	  "use-ssh-keys": True
    }
)
machine_credential_id = machine_credential_response['id']

configProfile = {
    "name": cluster_name,
    "datastax-version": datastax_version,
    "json": {
        "cassandra-yaml" : {
            "num_tokens" : vnodes,
            "authenticator" : "AllowAllAuthenticator",
            "authorizer" : "AllowAllAuthorizer",
#           "10-write-prom-conf" : {
#               "enabled" : True
#           }
#           ,
#           "client_encryption_options" : { "enabled" : True },
#           "server_encryption_options" : { "internode_encryption" : "all",
#               "require_client_auth" : False,
#		"require_endpoint_verification" : False
#           }
        },
#CLOSE cassandra.yaml
    },
    "comment": "LCM provisioned %s" % cluster_name
}


jvmOptions ={
   "additional-jvm-opts" : [
       "-Ddse.io.aio.enabled=false -Dcassandra.consistent.rangemovement=false"
   ]
}

if (vnodes == 1):
  configProfile["json"][jvmOptionsParam] = jvmOptions

print "configProfile: ", configProfile

cluster_profile_response = do_post("config_profiles/", configProfile)
cluster_profile_id = cluster_profile_response['id']

configProfileNoJava = {"name": cluster_name+"-no-java",
     "datastax-version": datastax_version,
     "json": {"cassandra-yaml" : {
         "num_tokens" : vnodes,
         "authenticator" : "AllowAllAuthenticator",
         "authorizer" : "AllowAllAuthorizer"
#,
#         "client_encryption_options" : { "enabled" : True },
#         "server_encryption_options" : { "internode_encryption" : "all",
#             "require_client_auth" : False,
#             "require_endpoint_verification" : False
#             }
         },
         "java-setup": {"manage-java": False}
         },
     "comment": "LCM provisioned %s" % cluster_name}

if (vnodes == 1) :
  configProfileNoJava["json"][jvmOptionsParam] = jvmOptions

cluster_profile_response_no_java = do_post("config_profiles/", configProfileNoJava)
cluster_profile_id_no_java = cluster_profile_response_no_java['id']

make_cluster_response = do_post("clusters/",
    {"name": cluster_name,
     "repository-id": repository_id,
     "machine-credential-id": machine_credential_id,
     "old-password": "cassandra",
     "new-password": cassandra_default_password,
     "config-profile-id": cluster_profile_id})

cluster_id = make_cluster_response['id']

data_centers = set()

print(server_list)
server_list = server_list.split(";")

for host in server_list:
    data_centers.add(host.split(":")[2])

data_center_ids = {}
for data_center in data_centers:
    make_dc_response = do_post("datacenters/",
        {"name": data_center,
         "cluster-id": cluster_id,
         "solr-enabled": True,
         "spark-enabled": True,
         "graph-enabled": True})
    dc_id = make_dc_response['id']
    data_center_ids[data_center] = dc_id

for host in server_list:
    node_ip = host.split(":")[0]
    private_ip = host.split(":")[1]
    data_center = host.split(":")[2]
    node_idx = host.split(":")[3]
    if (node_ip == server_ip):
        make_node_response = do_post("nodes/",
            {"name": "node" + str(node_idx) + "_" + node_ip,
             "config-profile-id": cluster_profile_id_no_java, 
             "listen-address": private_ip,
             "native-transport-address": "0.0.0.0",
                 "broadcast-address": node_ip,
             "native-transport-broadcast-address": node_ip,
             "ssh-management-address": node_ip,
             "datacenter-id": data_center_ids[data_center],
             "rack": "rack1"})
    else:
        make_node_response = do_post("nodes/",
            {"name": "node" + str(node_idx) + "_" + node_ip,
             "listen-address": private_ip,
             "native-transport-address": "0.0.0.0",
                 "broadcast-address": node_ip,
             "native-transport-broadcast-address": node_ip,
             "ssh-management-address": node_ip,
             "datacenter-id": data_center_ids[data_center],
             "rack": "rack1"})

# Request an install job to execute the installation and configuration of the
# cluster. Until this point, we've been describing future state. Now LCM will
# execute the changes necessary to achieve that state.
concurrencyStrategy = "default"
if (vnodes == 1):
  concurrencyStrategy = "cluster-at-a-time"


install_job = do_post("actions/install",
                     {"job-type":"install",
                      "job-scope":"cluster",
                      "resource-id":cluster_id,
#                      "concurrency-strategy": "cluster-at-a-time",
#                      "concurrency-strategy": "rack-per-dc-at-a-time",
                      "concurrency-strategy": concurrencyStrategy,
                      "continue-on-error":"false"})

print("http://%s:8888" % server_ip)
#open up a new browser tab that shows LCM working
webbrowser.open_new_tab('http://'+server_ip+':8888/opscenter/lcm.html')
