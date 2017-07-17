![iqas_logo](/src/main/resources/web/figures/iqas_logo_small.png?raw=true "iQAS logo")

# iQAS-platform

iQAS is an integration platform for QoO Assessment as a Service.

## System requirements

In order to correctly work, iQAS assumes that the following software have been correctly installed and are currently running:
* Java `1.8`
* Apache Zookeeper `3.4.9`
* Apache Kafka `0.10.2.0`
* MongoDB `v3.2.9`
* Apache Jena Fuseki `2.4.1`

This README describes installation and configuration of the iQAS platform for Unix-based operating systems (Mac and Linux).

## Project structure

```
project
│
└───logs
│   │   ...
│
└───src
    └───main
    │   └───java   
    │       └───config
    │       │   │ ...
    │       │       
    │       └───database
    │       │   │ ...
    │       │       
    │       └───kafka
    │       │   │ ...
    │       │       
    │       └───mapek
    │       │   │ ...
    │       │       
    │       └───model
    │       │   │ ...
    │       │       
    │       └───pipelines
    │       │   │ ...
    │       │       
    │       └───server
    │       │   │ ...
    │       │       
    │       └───utils
    │       │   │ ...
    │       │
    │       │   MainClass.java
    │
    │   └───resources
    │       └───web
    │       │   │   // Files for GUI
    │       │   
    │       │   application.conf
    │       │   iqas.properties
    │       │   logback.xml
    │       │   ontologies.yml
    │
    └───test
    │   │   // Modified Kafka tools classes used for benchmarking
    │   │   ConsumerPerformance.scala
    │   │   ProducerPerformance.scala
    │
    │   pom.xml
```

## Installation

1. Install and run the required third-party software:
    1. [Install Java](https://www.java.com/en/download/)
    2. [Install Kafka and Zookeeper](https://kafka.apache.org/quickstart)
    3. [Install MongoDB](https://www.mongodb.com/download-center)
    4. [Install Jena Fuseki](https://jena.apache.org/documentation/serving_data/)
2. Clone iQAS repository: <br/>`git clone https://github.com/antoineauger/iqas-platform.git`

In this quickstart guide, we will use the variable `$IQAS_ROOT` to refer to the emplacement of the directory `iqas-platform` you have just downloaded.

## Configuration

1. Java
    + Set `$JAVA_HOME` environment variable if not already set: <br/>`export JAVA_HOME="$(/usr/libexec/java_home)"`
2. Kafka
    1. Set JVM options for Kafka server: <br/>`export KAFKA_HEAP_OPTS="-Xms3g -Xmx3g"`<br/>More informations on JVM options can be found [here](http://www.oracle.com/technetwork/articles/java/vmoptions-jsp-140102.html)
    2. Start Zookeeper server
    3. Start Kafka server
3. Apache Jena Fuseki
    + Please follow the installation and configuration instructions of the Github project [iqas-ontology](https://github.com/antoineauger/iqas-ontology)



## iQAS interaction

### Graphical User Interface (GUI)

### RESTful APIs

### Binding to Kafka topic (for observation consumer)


## Usage

TODO

## Acknowledgments

The iQAS platform have been developed during the PhD thesis of Antoine Auger at ISAE-SUPAERO (2014-2017).

This research was supported in part by the French Ministry of Defence through financial support of the Direction Générale de l’Armement (DGA). 