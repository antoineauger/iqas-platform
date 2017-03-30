#!/bin/bash -e

echo "==== Setting proxy ISAE ===="

sudo echo 'export http_proxy="http://proxy.isae.fr:3128"' >> /etc/profile
sudo echo 'export https_proxy="http://proxy.isae.fr:3128"' >> /etc/profile
source /etc/profile
sudo echo 'Acquire::http::Proxy "http://proxy.isae.fr:3128";' >> /etc/apt/apt.conf.d/00aptitude

echo "[OK]"

echo "==== Performing Linux updates ===="

sudo apt-get -qq update
echo "[OK]"

echo "==== Checking for JAVA version installed ===="

if type -p java; then
    echo found java executable in PATH
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    echo found java executable in JAVA_HOME
    _java="$JAVA_HOME/bin/java"
else
    echo "no java"
    sudo apt-get -qq install openjdk-8-jre
    sudo apt-get -qq install openjdk-8-jdk

fi

if [[ "$_java" ]]; then
    version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo version "$version"
    if [[ "$version" > "1.7" ]]; then
        echo version is more than 1.7
    else
        echo version is less than 1.7
        sudo apt-get -qq install openjdk-8-jre
        sudo apt-get -qq install openjdk-8-jdk
    fi
fi

echo "==== Setting JAVA_HOME ===="

TEMP_PATH=$(readlink -f /usr/bin/java | sed "s:bin/java::")
sudo echo 'export JAVA_HOME="'$TEMP_PATH'"' >> /etc/profile
source /etc/profile
echo "[OK]"

echo "==== Downloading and decompressing Apache nifi ===="

cd 
wget http://apache.crihan.fr/dist/nifi/0.6.1/nifi-0.6.1-bin.tar.gz
tar xvzf nifi-0.6.1-bin.tar.gz
rm nifi-0.6.1-bin.tar.gz
echo "[OK]"

echo "==== Configuring UNIX system for Apache nifi ===="

sudo echo -e "*\thard\tnofile\t50000" >> /etc/security/limits.conf
sudo echo -e "*\tsoft\tnofile\t50000" >> /etc/security/limits.conf
sudo echo -e "*\thard\tnproc\t10000" >> /etc/security/limits.conf
sudo echo -e "*\tsoft\tnproc\t10000" >> /etc/security/limits.conf
sudo touch /etc/security/limits.d/90-nproc.conf
sudo echo -e "*\tsoft\tnproc\t10000" >> /etc/security/limits.d/90-nproc.conf
sudo sysctl -w net.ipv4.ip_local_port_range="10000 65000"
# sudo sysctl -w net.ipv4.netfilter.ip_conntrack_tcp_timeout_time_wait="1" # Do not work, to check
sudo echo "vm.swappiness = 0" >> /etc/sysctl.conf
echo "[OK]"

isNCM=true
NCMIPaddr="192.68.99.100"
NCMPort="33000"
slave1addr="192.68.99.101"
slave1Port="33001"
echo "==== Configuring ===="
echo -e "\n\n# iQAS configuration #" >> ./nifi-0.6.1/conf/nifi.properties
if [[ "$isNCM" = true ]]; then
    echo "This node is the NCM"
    echo "nifi.cluster.is.manager=true" >> ./nifi-0.6.1/conf/nifi.properties
    echo "nifi.cluster.manager.address="$NCMIPaddr >> ./nifi-0.6.1/conf/nifi.properties
    echo "nifi.cluster.manager.protocol.port="$NCMPort >> ./nifi-0.6.1/conf/nifi.properties
else
    echo "This node is a slave"
    echo "nifi.cluster.is.node=true" >> ./nifi-0.6.1/conf/nifi.properties
    echo "nifi.cluster.node.address="$slave1addr >> ./nifi-0.6.1/conf/nifi.properties
    echo "nifi.cluster.node.protocol.port="$slave1Port >> ./nifi-0.6.1/conf/nifi.properties
    echo "nifi.cluster.node.unicast.manager.address="$NCMIPaddr >> ./nifi-0.6.1/conf/nifi.properties
    echo "nifi.cluster.node.unicast.manager.protocol.port="$NCMPort >> ./nifi-0.6.1/conf/nifi.properties
fi


echo "==== Starting Apache nifi ===="

./nifi-0.6.1/bin/nifi.sh start

echo "Apache nifi is now running, point your browser on one of these addresses:" 
echo ifconfig | awk -F "[: ]+" '/inet addr:/ { if ($4 != "127.0.0.1") print "http://"$4":8080/nifi/" }'

echo "==== TODO tasks list ===="

echo ">>>> TODO: Please also add the noatime option to each partition of /etc/fstab to increase throughput"


