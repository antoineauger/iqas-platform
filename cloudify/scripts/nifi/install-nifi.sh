#!/bin/bash

set -e

ctx logger info "Setting up proxy ISAE"
#echo 'export http_proxy="http://proxy.isae.fr:3128"' | sudo tee -a /etc/profile
#echo 'export https_proxy="http://proxy.isae.fr:3128"' | sudo tee -a /etc/profile
#source /etc/profile
#echo 'Acquire::http::Proxy "http://proxy.isae.fr:3128";' | sudo tee -a /etc/apt/apt.conf.d/00aptitude

#################

ctx logger info "Performing Linux updates"
sudo apt-get -qq update

#################

ctx logger info "Checking JAVA version"
if type -p java; then
    echo found java executable in PATH
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    echo found java executable in JAVA_HOME
    _java="$JAVA_HOME/bin/java"
else
    ctx logger warning "Unable to find a suitable JAVA version"
    ctx logger info "Installation of JAVA 1.7"
    sudo apt-get -qq install openjdk-7-jre
    sudo apt-get -qq install openjdk-7-jdk
fi

if [[ "$_java" ]]; then
    version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo version "$version"
    if [[ "$version" > "1.6" ]]; then
        ctx logger info "Found a suitable JAVA version (found version: ${version})"
    else
        ctx logger warning "Unable to find a suitable JAVA version, JAVA version is less than 1.7 (found version: ${version})"
        ctx logger info "Installation of JAVA 1.7"
        sudo apt-get -qq install openjdk-7-jre
        sudo apt-get -qq install openjdk-7-jdk
    fi
fi

#################

ctx logger info "Setting JAVA_HOME env variable"
TEMP_PATH=$(readlink -f /usr/bin/java | sed "s:bin/java::")
echo 'export JAVA_HOME="'$TEMP_PATH'"' | sudo tee -a /etc/profile
source /etc/profile
ctx logger info "JAVA_HOME=${TEMP_PATH}"

#################

NIFI_VERSION=$(ctx node properties nifi_version)
NIFI_URL="http://apache.crihan.fr/dist/nifi/${NIFI_VERSION}/nifi-${NIFI_VERSION}-bin.tar.gz"
TAR_ARCHIVE="nifi-${NIFI_VERSION}-bin.tar.gz"
USER_DIR=/home/$USER
NIFI_ROOT_PATH=${USER_DIR}/nifi-${NIFI_VERSION}

cd ${USER_DIR}
if [ -d "${NIFI_ROOT_PATH}" ]; then
        ctx logger info "Nifi already installed, No need to download"
else
    ctx logger info "Downloading Apache nifi at ${NIFI_URL}"

    set +e
    curl_cmd=$(which curl)
    wget_cmd=$(which wget)
    set -e

    if [[ ! -z ${curl_cmd} ]]; then
        curl -s -L -o ${TAR_ARCHIVE} ${NIFI_URL}
    elif [[ ! -z ${wget_cmd} ]]; then
        wget -q -O ${NIFI_URL}
    else
        ctx logger error "Failed to download ${NIFI_URL}: Neither 'cURL' nor 'wget' were found on the system"
        exit 1;
    fi

    ctx logger info "Untaring Apache nifi ${TAR_ARCHIVE}"
    tar xvzf ${TAR_ARCHIVE}
    rm ${TAR_ARCHIVE}
fi

#################

ctx logger info "Apache nifi installation complete"

