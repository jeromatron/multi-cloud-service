FROM ubuntu:18.04
MAINTAINER Sebastian Estevez estevezsebastian@gmail.com

# Install all apt-get utils and required repos
RUN apt-get update && \
    apt-get upgrade -y && \
    # Install add-apt-repository
    apt-get install -y \
         # default-jdk i.e. openjdk 11
        software-properties-common curl apt-transport-https python-pip groff less jq default-jdk  && \
    apt-get update

# Azure sources.list
RUN AZ_REPO=$(lsb_release -cs); echo "deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ $AZ_REPO main" | \
    tee /etc/apt/sources.list.d/azure-cli.list

RUN curl -L https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

# Google sources list
RUN export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -


    # Install
RUN apt-get update && apt-get install -y \
	  git maven \
    apt-transport-https azure-cli \
    google-cloud-sdk

RUN java -version

# AWS cli
RUN pip install awscli --upgrade --user
RUN echo 'PATH=/root/.local/bin/:$PATH' >> /root/.bashrc
ENV PATH=/root/.local/bin/:${PATH}

COPY ./config config
COPY ./config/.aws/ /root/.aws

# Check login AWS
#RUN aws ec2 describe-instances

# Login gcloud
RUN gcloud auth activate-service-account --key-file=/config/gcloud/gcloud-service-key.json
RUN cat /config/gcloud/gcloud-service-key.json | jq ".project_id" | xargs gcloud config set project 
#RUN gcloud config set project asset-hub-168516
# gcloud auth activate-service-account asset-hub@asset-hub-168516.iam.gserviceaccount.com --key-file=../../config/gcloud/gcloud-service-key.json

# Login azure
#ARG AZ_TENNANT=$(head -1 config/azure/creds.txt)
#ARG AZ_APP_ID=$(tail -2 config/azure/creds.txt | head -1)
#ARG AZ_KEY=$(tail -1 config/azure/creds.txt)
#RUN az login -u $AZ_USER -p $AZ_PASSWORD
RUN cat config/azure/creds.txt | awk '{system( "az login --service-principal -u " $2 " -p " $3 " --tenant " $1 )}'
#echo az login --service-principal -u $AZ_APP_ID -p $AZ_KEY --tenant $AZ_TENNANT

#RUN git clone https://github.com/jeromatron/multi-cloud-service.git
COPY ./ /multi-cloud-service/

RUN chmod 600 /multi-cloud-service/config/assethub-2019

# TODO: use the precompiled version from the copy ^^
RUN cd multi-cloud-service/app && mvn clean package

# lcm script dependencies
RUN pip install -r multi-cloud-service/requirements.txt

CMD . config/academy && cd multi-cloud-service/app && java -jar target/multi-cloud-service.jar server conf/multi-cloud-service-conf.yaml
#RUN cd /multi-cloud-service/iaas && ./deploy_aws.sh
#RUN /multi-cloud-service/iaas/deploy_azure.sh
#RUN /multi-cloud-service/iaas/deploy_gcp.sh
