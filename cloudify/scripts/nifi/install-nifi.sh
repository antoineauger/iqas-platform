#!/bin/bash

set -e

ctx logger info "Setting up proxy ISAE"
sudo echo 'export http_proxy="http://proxy.isae.fr:3128"' >> /etc/profile
sudo echo 'export https_proxy="http://proxy.isae.fr:3128"' >> /etc/profile
source /etc/profile
sudo echo 'Acquire::http::Proxy "http://proxy.isae.fr:3128";' >> /etc/apt/apt.conf.d/00aptitude

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
    ctx logger info "Unable to find a suitable JAVA version"
    ctx logger info "Installation of JAVA 1.8"
    sudo apt-get -qq install openjdk-8-jre
    sudo apt-get -qq install openjdk-8-jdk

fi

if [[ "$_java" ]]; then
    version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo version "$version"
    if [[ "$version" > "1.7" ]]; then
        ctx logger info "Found a suitable JAVA version (found version: ${version})"
    else
        ctx logger info "Unable to find a suitable JAVA version, JAVA version is less than 1.7 (found version: ${version})"
        ctx logger info "Installation of JAVA 1.8"
        sudo apt-get -qq install openjdk-8-jre
        sudo apt-get -qq install openjdk-8-jdk
    fi
fi

#################

ctx logger info "Setting JAVA_HOME env variable"
temp_path=$(readlink -f /usr/bin/java | sed "s:bin/java::")
sudo echo 'export JAVA_HOME="'$temp_path'"' >> /etc/profile
source /etc/profile
ctx logger info "JAVA_HOME=${temp_path}"

#################

export NIFI_VERSION=$(ctx node properties nifi_version)
url='http://apache.crihan.fr/dist/nifi/${nifi_version}/nifi-${nifi_version}-bin.tar.gz'
tar_archive='nifi-${nifi_version}-bin.tar.gz'
ctx logger info "Downloading Apache nifi at ${url}"
cd ~

set +e
curl_cmd=$(which curl)
wget_cmd=$(which wget)
set -e

if [[ ! -z ${curl_cmd} ]]; then
    curl -L -o ${url}
elif [[ ! -z ${wget_cmd} ]]; then
    wget -O ${url}
else
    ctx logger error "Failed to download ${url}: Neither 'cURL' nor 'wget' were found on the system"
    exit 1;
fi

#################

ctx logger info "Untaring Apache nifi ${tar_archive}"
tar xvzf nifi-0.6.1-bin.tar.gz
rm nifi-0.6.1-bin.tar.gz

#################

ctx logger info "Apache nifi installation complete"

