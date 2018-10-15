![iqas_logo](/src/main/resources/web/figures/iqas_logo_small.png?raw=true "iQAS logo")

# iQAS-platform

iQAS is an integration platform for QoO Assessment as a Service.

## The iQAS ecosystem

In total, 5 Github projects form the iQAS ecosystem:
1. [iqas-platform](https://github.com/antoineauger/iqas-platform) (this project)<br/>The QoO-aware platform that allows consumers to integrate many observation sources, submit requests with QoO constraints and visualize QoO in real-time.
2. [virtual-sensor-container](https://github.com/antoineauger/virtual-sensor-container) <br/>A shippable Virtual Sensor Container (VSC) Docker image for the iQAS platform. VSCs allow to generate observations at random, from file, or to retrieve them from the Web.
3. [virtual-app-consumer](https://github.com/antoineauger/virtual-app-consumer) <br/>A shippable Virtual Application Consumers (VAC) Docker image for the iQAS platform. VACs allow to emulate fake consumers that submit iQAS requests and consume observations while logging the perceived QoO in real-time.
4. [iqas-ontology](https://github.com/antoineauger/iqas-ontology) <br/>Ontological model and examples for the QoOonto ontology, the core ontology used by iQAS.
5. [iqas-pipelines](https://github.com/antoineauger/iqas-pipelines) <br/>An example of a custom-developed QoO Pipeline for the iQAS platform.

## System requirements

In order to correctly work, iQAS assumes that the following software has been correctly installed and are currently running. We indicate between parenthesis the software versions used for development and test:
* Java (`1.8`)
* Apache Maven (`3.3.9`)
* Apache Zookeeper (`3.4.9`)
* Apache Kafka (`0.10.2.0`)
* MongoDB (`v3.2.9`)
* Apache Jena Fuseki (`2.4.1`)

This README describes installation and configuration of the iQAS platform for Unix-based operating systems (Mac and Linux).

## Project structure

```
project
│
└───logs
│
└───src
    └───main
    │   └───java   
    │   │   │   // config, database, kafka, mapek,
    │   │   │   // model, pipelines, server and utils packages
    │   │   │
    │   │   │   MainClass.java
    │   │
    │   └───resources
    │       └───web
    │       │   │   // Files for GUI
    │       │   
    │       │   // Configuration files
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
    2. [Install Maven](https://maven.apache.org/download.cgi)
    3. [Install Kafka and Zookeeper](https://kafka.apache.org/quickstart)
    4. [Install MongoDB](https://www.mongodb.com/download-center)
    5. [Install Jena Fuseki](https://jena.apache.org/documentation/serving_data/)
2. Clone iQAS repository: <br/>`git clone https://github.com/antoineauger/iqas-platform.git`

In this quickstart guide, we will use the variable `$IQAS_DIR` to refer to the emplacement of the directory `iqas-platform` you have just downloaded.

## Configuration

1. Java
    + Export (or set in your `.bashrc`) the `$JAVA_HOME` environment variable if not already set: <br/>`export JAVA_HOME="$(/usr/libexec/java_home)"`
2. Maven
    + Add the `apache-maven-X.X.X/bin` directory to your `$PATH`such that you will be able to run the command `mvn` anywhere.
3. Kafka
    1. Export (or set in your `.bashrc`) the JVM options for Kafka server: <br/>`export KAFKA_HEAP_OPTS="-Xms3g -Xmx3g"`<br/>Remember to adapt Kafka options to your hardware, more informations on JVM options can be found [here](http://www.oracle.com/technetwork/articles/java/vmoptions-jsp-140102.html)
    2. Start Zookeeper server<br/>`$KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties`             
    3. Start Kafka server<br/>`$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties`
4. MongoDB
    + Start MongoDB service:<br/>`sudo $MONGODB_DIR/bin/mongod -f your_config_file.conf`
5. Apache Jena Fuseki
    + Please follow the installation and configuration instructions of the Github project [iqas-ontology](https://github.com/antoineauger/iqas-ontology)
6. iQAS
    1. Set the paths of the different ontology files that need to be imported in the configuration file `ontologies.yml`
    2. Set deployment options for iQAS in the configuration file `iqas.properties`
    3. Compile project with maven:<br/>`mvn -T C2.0 clean install -DskipTests`
    4. Run iQAS platform:<br/>`java -server -d64 -Xms2048m -Xmx8192m -XX:+UseParallelOldGC -cp target/iqas-platform-1.0-SNAPSHOT-allinone.jar fr.isae.iqas.MainClass`

The following table list the available options in case you want to add new entities to the iQAS platform:

|     entity    | GUI |  QoOnto ontology update | RESTful endpoints |
|:-------------:|:---:|:-----------------------:|:-----------------:|
|     sensor    |  X  |            X            |                   |
|  QoO Pipeline |  X  |            X            |                   |
|    request    |  X  |                         |         X         |
| QoO Attribute |     | X (+source code update) |                   |
|     place     |     |            X            |                   |
|     topic     |     |            X            |                   |

## Submitting a new iQAS Request

### Through the Graphical User Interface (GUI)

By default, the iQAS GUI can be accessed at [http://\[api_gateway_endpoint_address\]:\[api_gateway_endpoint_port\]](#)
![iqas_logo](/src/main/resources/web/figures/screenshot_iqas_homepage.png?raw=true "iQAS homepage")

### Using RESTful APIs

You can also perform POST, DELETE and GET queries on the RESTful endpoints provided by the platform. Here is a list of allowed operations:

#### GET

For sensors:

+ /sensors
+ /sensors/\[sensor_id\]

For requests:

+ /requests
+ /requests/\[request_id\]

For QoO Pipelines:

+ /pipelines
+ /pipelines/\[pipeline_id\]
+ /pipelines?print=ids

For QoO attributes:

+ /qoo/attributes
+ /qoo/custom_params

For places:

+ /places
+ /places?nearTo=\[location\]

#### POST

+ /requests

You can submit new requests to the endpoint `/requests`. You should set the header variable `Content-Type: application/json`.<br/>For instance, following iQAS requests are valid:

```javascript
// Simple request without QoO constraints (Raw Data level)
{
	"application_id": "testApplication",
	"topic": "ALL",
	"location": "ALL",
	"obs_level": "RAW_DATA"
}
```

```javascript
// More complex request with QoO interest (Information level)
{
	"application_id": "testApplication",
	"topic": "ALL",
	"location": "ALL",
	"obs_level": "INFORMATION",
	"qoo": {
	    "interested_in": ["OBS_ACCURACY", "OBS_FRESHNESS"]
	}
}
```

```javascript
// More complex request with QoO interest and QoO constraints (Knowledge level)
{
	"application_id": "testApplication",
	"topic": "temperature",
	"location": "Toulouse",
	"obs_level": "KNOWLEDGE",
	"qoo": {
        "interested_in": ["OBS_ACCURACY", "OBS_FRESHNESS"],
        "iqas_params": {
            "obsRate_min": "5/s"
        },
        "sla_level": "GUARANTEED"
    }
}
```

When present, the `qoo` parameter represents a "QoO Service Level Agreement" and should be expressed as follow:
+ `interested_in` contains an ordered list (first=very important, last=less important) of QoO attributes that matter for the given application.
+ `iqas_params` can be used to specify some additional QoO requirements that the iQAS platform natively supports.
+ `custom_params` can be used to specify additional QoO requirements that have been set as customizable for a custom QoO Pipeline.

#### DELETE

+ /requests/\[request_id\]

## Observation consumption by applications

Once an iQAS request has successfully been enforced, observations are available to applications at the Kafka topic `[application_id]_[request_id]`. Here are some examples of observations that may be delivered according to the observation level asked:

+ `RAW_DATA`
```javascript
{
  "date" : 1500364730089,
  "value" : 4.276,
  "producer" : "sensor01",
  "timestamps" : "produced:1500364730089;iQAS_in:1500364730543;iQAS_out:1500364730664",
  "qoOAttributeValues" : {
    "OBS_ACCURACY" : "100.0",
    "OBS_FRESHNESS" : "575.0"
  }
}
```

+ `INFORMATION`
```javascript
{
  "date" : 1500364775129,
  "value" : -7.29,
  "producer" : "sensor01",
  "timestamps" : "produced:1500364775129;iQAS_in:1500364775531;iQAS_out:1500364776021",
  "qoOAttributeValues" : {
    "OBS_ACCURACY" : "100.0",
    "OBS_FRESHNESS" : "892.0"
  },
  "sensorContext" : {
    "latitude" : "43.53101809",
    "longitude" : "1.40158296",
    "altitude" : "152",
    "relativeLocation" : "Chullanka - Portet-sur-Garonne",
    "topic" : "temperature"
  }
}
```

+ `KNOWLEDGE`
```javascript
{"obs": [
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#accuracyKind",
    "@type": "http://purl.org/iot/vocab/m3-lite#Others"
  },
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#accuracyUnit",
    "@type": "http://purl.org/iot/vocab/m3-lite#Percent"
  },
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#freshnessKind",
    "@type": "http://purl.org/iot/vocab/m3-lite#Others"
  },
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#freshnessUnit",
    "@type": "http://purl.org/iot/vocab/m3-lite#Millisecond"
  },
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#location",
    "@type": "http://www.w3.org/2003/01/geo/wgs84_pos#Point",
    "alt": "152",
    "altRelative": "0",
    "lat": "43.53101809",
    "long": "1.40158296",
    "relativeLocation": "Chullanka - Portet-sur-Garonne"
  },
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#obs",
    "@type": "http://purl.oclc.org/NET/ssnx/ssn#Observation",
    "featureOfInterest": "http://isae.fr/iqas/qoo-ontology#publicLocations",
    "observationResult": "http://isae.fr/iqas/qoo-ontology#sensorOutput",
    "observedBy": "http://isae.fr/iqas/qoo-ontology#sensor01",
    "observedProperty": "http://isae.fr/iqas/qoo-ontology#temperature"
  },
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#obsValue",
    "@type": "http://purl.oclc.org/NET/ssnx/ssn#ObservationValue",
    "hasQoO": "http://isae.fr/iqas/qoo-ontology#qooAttributesList",
    "hasQuantityKind": "http://purl.org/iot/vocab/m3-lite#Temperature",
    "hasUnit": "http://purl.org/iot/vocab/m3-lite#DegreeCelsius",
    "obsDateValue": "2017-07-18 10:00:00.147",
    "obsLevelValue": "KNOWLEDGE",
    "obsStrValue": "9.383",
    "obsTimestampsValue": "produced:1500364800147;iQAS_in:1500364800313;iQAS_out:1500364800645"
  },
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#qooAttributesList",
    "@type": "rdf:Seq",
    "_1": "http://isae.fr/iqas/qoo-ontology#qooIntrinsic_OBS_ACCURACY",
    "_2": "http://isae.fr/iqas/qoo-ontology#qooIntrinsic_OBS_FRESHNESS"
  },
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#qooIntrinsic_OBS_ACCURACY",
    "@type": "http://isae.fr/iqas/qoo-ontology#QoOIntrisicQuality",
    "hasQoOValue": "http://isae.fr/iqas/qoo-ontology#qooValue_OBS_ACCURACY",
    "isAbout": "OBS_ACCURACY"
  },
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#qooIntrinsic_OBS_FRESHNESS",
    "@type": "http://isae.fr/iqas/qoo-ontology#QoOIntrisicQuality",
    "hasQoOValue": "http://isae.fr/iqas/qoo-ontology#qooValue_OBS_FRESHNESS",
    "isAbout": "OBS_FRESHNESS"
  },
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#qooValue_OBS_ACCURACY",
    "@type": "http://isae.fr/iqas/qoo-ontology#QoOValue",
    "hasQuantityKind": "http://isae.fr/iqas/qoo-ontology#accuracyKind",
    "hasUnit": "http://isae.fr/iqas/qoo-ontology#accuracyUnit",
    "qooStrValue": "100.0"
  },
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#qooValue_OBS_FRESHNESS",
    "@type": "http://isae.fr/iqas/qoo-ontology#QoOValue",
    "hasQuantityKind": "http://isae.fr/iqas/qoo-ontology#freshnessKind",
    "hasUnit": "http://isae.fr/iqas/qoo-ontology#freshnessUnit",
    "qooStrValue": "497.0"
  },
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#sensor01",
    "@type": "http://purl.oclc.org/NET/ssnx/ssn#Sensor",
    "location": "http://isae.fr/iqas/qoo-ontology#location"
  },
  {
    "@id": "http://isae.fr/iqas/qoo-ontology#sensorOutput",
    "@type": "http://purl.oclc.org/NET/ssnx/ssn#SensorOutput",
    "hasValue": "http://isae.fr/iqas/qoo-ontology#obsValue"
  }
]}
```

## Performance evaluation and benchmarking

All experiments were performed on a Mac Pro (2013) with 3.7 GHz Quad-Core Intel Xeon E5 processor and 32 Go RAM (DDR3).

2 different configurations were envisioned:
1. The `initial_config` has been used to perform overhead evaluation (end-to-end delay and iQAS delay).
2. The `high_throughput_config` has been used to benchmark maximum throughput that the iQAS platform could achieve for both observation ingestion and observation delivery.

All configuration files can be found in `$IQAS_DIR/src/test` for anyone interested in reproducing the results. 

Kafka and Zookeeper were configured with `$IQAS_DIR/src/test/0_common_configuration/server.properties` and `$IQAS_DIR/src/test/0_common_configuration/zookeeper.properties` files, respectively. 
We set the following JVM options for Kafka:
```
export KAFKA_HEAP_OPTS="-Xms3g -Xmx3g"
```

For each observation level, we submitted requests 1) without and 2) with QoO constraints.
Detail of the submitted requests may be found [here](https://gist.github.com/antoineauger).

#### iQAS overhead evaluation

We used one Virtual Sensor Container (VSC) that output random temperature measurements (rate=1 observation every 5 seconds) and one Virtual Application Consumer (VAC).

Results are coming soon...

#### iQAS throughput evaluation

We used a modified version of Kafka tools `ProducerPerformance.scala` and `ConsumerPerformance.scala` (see classes in `$IQAS_DIR/src/test`) to produce and consume 500000 messages.

For each experiment, we performed the following steps:
1. We start the iQAS platform and reset Kafka topics
2. We submit one iQAS request and note down the `request_id`.
3. We start the ProducerPerformance tool:
    ```
    ./bin/kafka-run-class.sh kafka.tools.ProducerPerformance --topics temperature --broker-list 10.161.3.181:9092 --message-size 192 --messages 500000 --new-producer
    ```
4. Immediately after, we start ConsumerPerformance tool:
    ```
    ./bin/kafka-run-class.sh kafka.tools.ConsumerPerformance --topic app2_[request_id] --broker-list 10.161.3.181:9092 --messages 500000 --new-consumer
    ```

All results are given for 5 independent simulation runs.

Results are coming soon...

## QoO Pipelines

A custom QoO Pipeline must extends `AbstracPipeline` class and implements the `IPipeline` interface (see below). For more information about QoO Pipelines and see a development walk-through, please visit the Github project [iqas-pipelines](https://github.com/antoineauger/iqas-pipelines).

```java
import static fr.isae.iqas.model.request.Operator.NONE;

public class CustomPipeline extends AbstractPipeline implements IPipeline {

    public CustomPipeline() {
        super("Custom Pipeline", "CustomPipeline", true);
        addSupportedOperator(NONE);
        setParameter("nb_copies", String.valueOf(1), true);
    }

    @Override
    public Graph<FlowShape<ConsumerRecord<byte[], String>, ProducerRecord<byte[], String>>, Materializer> getPipelineGraph() {

        final ObservationLevel askedLevelFinal = getAskedLevel();
        Graph runnableGraph = GraphDSL
                .create(builder -> {

                    // ################################# Example of QoO Pipeline logic #################################

                    final FlowShape<ConsumerRecord, RawData> consumRecordToRawData = builder.add(
                            Flow.of(ConsumerRecord.class).map(r -> {
                                JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                return new RawData(
                                        sensorDataObject.getString("date"),
                                        sensorDataObject.getString("value"),
                                        sensorDataObject.getString("producer"),
                                        sensorDataObject.getString("timestamps"));
                            })
                    );

                    final FlowShape<RawData, ProducerRecord> rawDataToProdRecord = builder.add(
                            Flow.of(RawData.class).map(r -> {
                                ObjectMapper mapper = new ObjectMapper();
                                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                                return new ProducerRecord<byte[], String>(getTopicToPublish(), mapper.writeValueAsString(r));
                            })
                    );

                    builder.from(consumRecordToRawData.out())
                            .via(builder.add(new CloneSameValueGS<RawData>(Integer.valueOf(getParams().get("nb_copies")))))
                            .toInlet(rawDataToProdRecord.in());

                    // ################################# End of QoO Pipeline logic #################################

                    return new FlowShape<>(consumRecordToRawData.in(), rawDataToProdRecord.out());

                });

        return runnableGraph;
    }

    @Override
    public String getPipelineID() {
        return getClass().getSimpleName();
    }
}
```

## Logging

iQAS use slf4j over logback to log its activity in `$IQAS_DIR/logs`. A new log file is created for each run of the platform.<br/>
You can customize logs by editing the configuration file `logback.yml`.

## Configuration of 3rd-party software

iQAS relies on both [Akka toolkit](http://akka.io/) and [Akka Streams Kafka](http://doc.akka.io/docs/akka-stream-kafka/current/home.html).
Specific options for Akka are located in the configuration file `$IQAS_DIR/src/main/resources/applications.conf`
For more information, visit the [Akka documentation](http://doc.akka.io/docs/akka/current/java/general/configuration.html) section about configuration.

## Acknowledgments

The iQAS platform have been developed during the PhD thesis of [Antoine Auger](https://antoineauger.fr/) at ISAE-SUPAERO (2014-2017).

This research was supported in part by the French Ministry of Defence through financial support of the Direction Générale de l’Armement (DGA).

![banniere](/src/main/resources/web/figures/banniere.png?raw=true "Banniere")
