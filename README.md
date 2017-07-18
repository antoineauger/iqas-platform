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

In this quickstart guide, we will use the variable `$IQAS_DIR` to refer to the emplacement of the directory `iqas-platform` you have just downloaded.

## Configuration

1. Java
    + Export (or set in your `.bashrc`) the `$JAVA_HOME` environment variable if not already set: <br/>`export JAVA_HOME="$(/usr/libexec/java_home)"`
2. Kafka
    1. Export (or set in your `.bashrc`) the JVM options for Kafka server: <br/>`export KAFKA_HEAP_OPTS="-Xms3g -Xmx3g"`<br/>Remember to adapt Kafka options to your hardware, more informations on JVM options can be found [here](http://www.oracle.com/technetwork/articles/java/vmoptions-jsp-140102.html)
    2. Start Zookeeper server<br/>`$KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties`             
    3. Start Kafka server<br/>`$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties`
3. MongoDB
    + Start MongoDB service:<br/>`sudo $MONGODB_DIR/bin/mongod -f your_config_file.conf`
4. Apache Jena Fuseki
    + Please follow the installation and configuration instructions of the Github project [iqas-ontology](https://github.com/antoineauger/iqas-ontology)
5. iQAS
    1. TODO


## iQAS interaction

### Graphical User Interface (GUI)

By default, the iQAS GUI can be accessed at [http://\[api_gateway_endpoint_address\]:\[api_gateway_endpoint_port\]](#)

### RESTful APIs

You can perform POST, DELETE and GET queries on the RESTful endpoints provided by the platform. Here is a list of allowed operations:

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

### Observation consumption by applications

Once an iQAS request has been successfully enforced, observations are available to applications at the Kafka topic `[application_id]_[request_id]`.

## Performance evaluation and benchmarking

TODO

## QoO Pipeline development walk-through

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

## Other satellite projects for the iQAS platform

TODO

## Acknowledgments

The iQAS platform have been developed during the PhD thesis of [Antoine Auger](https://personnel.isae-supaero.fr/antoine-auger/?lang=en) at ISAE-SUPAERO (2014-2017).

This research was supported in part by the French Ministry of Defence through financial support of the Direction Générale de l’Armement (DGA). 