package fr.isae.iqas.database;

import fr.isae.iqas.model.jsonld.*;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 25/01/2017.
 */
// TODO to revise after ontology changes
public class FusekiController {
    private Logger log = LoggerFactory.getLogger(FusekiController.class);

    private String sparqlService = null;
    private String baseStringForRequests = null;
    private Map<String, String> namespaces = null;

    public FusekiController(Properties prop) {
        this.sparqlService = prop.getProperty("fuseki_query_endpoint");

        this.namespaces = new ConcurrentHashMap<>();
        this.namespaces.put("ssn", prop.getProperty("ssnIRI"));
        this.namespaces.put("iot-lite", prop.getProperty("iotliteIRI"));
        this.namespaces.put("qoo", prop.getProperty("qooIRI"));
        this.namespaces.put("geo", prop.getProperty("geoIRI"));
        this.namespaces.put("rdf", prop.getProperty("rdfIRI"));

        this.baseStringForRequests = "PREFIX ssn: <" + namespaces.get("ssn") + ">\n";
        this.baseStringForRequests += "PREFIX iot-lite: <" + namespaces.get("iot-lite") + ">\n";
        this.baseStringForRequests += "PREFIX qoo: <" + namespaces.get("qoo") + ">\n";
        this.baseStringForRequests += "PREFIX geo: <" + namespaces.get("geo") + ">\n";
        this.baseStringForRequests += "PREFIX rdf: <" + namespaces.get("rdf") + ">\n";

        log.info("FusekiController successfully created!");
    }

    /**
     * Sensors
     */

    VirtualSensorList _findAllSensors() {
        Map<String, VirtualSensor> processedSensors = new ConcurrentHashMap<>();
        QuerySolution binding;
        VirtualSensorList sensorList = new VirtualSensorList();
        sensorList.sensors = new ArrayList<>();

        String req = baseStringForRequests +
                "SELECT ?sensor ?topic ?url ?longitude ?latitude ?relativeLocation\n" +
                "WHERE {\n" +
                "?sensor rdf:type ssn:Sensor .\n" +
                "?sensor ssn:madeObservation ?o .\n" +
                "?o ssn:observedProperty ?topic .\n" +
                "?topic qoo:canBeRetrievedThrough ?s .\n" +
                "?s iot-lite:endpoint ?url .\n" +
                "?sensor geo:location ?pos .\n" +
                "?pos geo:long ?longitude .\n" +
                "?pos geo:lat ?latitude .\n" +
                "?pos iot-lite:relativeLocation ?relativeLocation\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();

                Resource id = (Resource) binding.get("sensor");
                Resource topic = (Resource) binding.get("topic");
                Literal url = binding.getLiteral("url");
                Literal longitude = binding.getLiteral("longitude");
                Literal latitude = binding.getLiteral("latitude");
                Literal relativeLocation = binding.getLiteral("relativeLocation");

                VirtualSensor sensorTemp;
                if (processedSensors.containsKey(id.getURI())) {
                    sensorTemp = processedSensors.get(id.getURI());
                    ServiceEndpoint endpointTemp = new ServiceEndpoint();
                    endpointTemp.topic = topic.getURI();
                    endpointTemp.url = url.getString();
                    sensorTemp.endpoints.add(endpointTemp);
                }
                else {
                    sensorTemp = new VirtualSensor();
                    sensorTemp.sensor_id = id.getURI();

                    Location locTemp = new Location();
                    locTemp.latitude = latitude.getString();
                    locTemp.longitude = longitude.getString();
                    locTemp.relative_location = relativeLocation.getString();

                    sensorTemp.endpoints = new ArrayList<>();
                    ServiceEndpoint endpointTemp = new ServiceEndpoint();
                    endpointTemp.topic = topic.getURI();
                    endpointTemp.url = url.getString();
                    sensorTemp.endpoints.add(endpointTemp);
                    sensorTemp.location = locTemp;
                }
                processedSensors.put(id.getURI(), sensorTemp);
            }

            sensorList.sensors.addAll(processedSensors.values());
        }

        if (sensorList.sensors.size() == 0) {
            return null;
        }
        else {
            return sensorList;
        }
    }

    VirtualSensor _findSpecificSensor(String sensor_id) {
        QuerySolution binding = null;
        VirtualSensor sensor = new VirtualSensor();

        String req = baseStringForRequests +
                "SELECT ?sensor ?topic ?url ?longitude ?latitude ?relativeLocation\n" +
                "WHERE {\n" +
                "?sensor iot-lite:id \"" + sensor_id + "\" .\n" +
                "?sensor ssn:madeObservation ?o .\n" +
                "?o ssn:observedProperty ?topic .\n" +
                "?topic qoo:canBeRetrievedThrough ?s .\n" +
                "?s iot-lite:endpoint ?url .\n" +
                "?sensor geo:location ?pos .\n" +
                "?pos geo:long ?longitude .\n" +
                "?pos geo:lat ?latitude .\n" +
                "?pos iot-lite:relativeLocation ?relativeLocation\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            if (r.hasNext()) {
                binding = r.nextSolution();

                Resource id = (Resource) binding.get("sensor");
                Resource topic = (Resource) binding.get("topic");
                Literal url = binding.getLiteral("url");
                Literal longitude = binding.getLiteral("longitude");
                Literal latitude = binding.getLiteral("latitude");
                Literal relativeLocation = binding.getLiteral("relativeLocation");

                sensor.sensor_id = id.getURI();

                Location locTemp = new Location();
                locTemp.latitude = latitude.getString();
                locTemp.longitude = longitude.getString();
                locTemp.relative_location = relativeLocation.getString();
                sensor.location = locTemp;

                sensor.endpoints = new ArrayList<>();
                ServiceEndpoint endpointTemp = new ServiceEndpoint();
                endpointTemp.topic = topic.getURI();
                endpointTemp.url = url.getString();
                sensor.endpoints.add(endpointTemp);
            }
        }

        if (binding == null) {
            return null;
        }
        else {
            return sensor;
        }
    }

    /**
     * Topics
     */

    TopicList _findAllTopics() {
        QuerySolution binding;
        Set<String> topicAlreadyAdded = new HashSet<>();
        TopicList topicList = new TopicList();
        topicList.topics = new ArrayList<>();

        String req = baseStringForRequests +
                "SELECT ?topic\n" +
                "WHERE {\n" +
                "?topic rdf:type ssn:Property\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();
                Resource topic = (Resource) binding.get("topic");

                if (!topicAlreadyAdded.contains(topic.getURI())) {
                    Topic t = new Topic();
                    t.topic = topic.getURI();
                    topicList.topics.add(t);
                    topicAlreadyAdded.add(topic.getURI());
                }
            }
        }

        if (topicList.topics.size() == 0) {
            return null;
        }
        else {
            return topicList;
        }
    }

    // TODO to finish
    Topic _findSpecificTopic(String topic_id) {
        QuerySolution binding = null;
        Topic topic = new Topic();

        String req = baseStringForRequests +
                "SELECT ?topic ?service ?level\n" +
                "WHERE {\n" +
                topic_id + " rdf:type ssn:Property .\n" +
                topic_id + " qoo:canBeRetrievedThrough ?service .\n" +
                topic_id + " qoo:obsLevelValue ?level\n" +
                "}";

        //System.out.println(req);

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            if (r.hasNext()) {
                binding = r.nextSolution();
                Resource topicRes = (Resource) binding.get("service");


                topic.topic = topicRes.getURI();
            }
        }

        if (binding == null) {
            return null;
        }
        else {
            return topic;
        }
    }
}
