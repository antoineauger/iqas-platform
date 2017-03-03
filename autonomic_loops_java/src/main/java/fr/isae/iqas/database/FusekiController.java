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

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
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

    public VirtualSensorList _findAllSensors() {
        Map<String, VirtualSensor> processedSensors = new ConcurrentHashMap<>();
        QuerySolution binding;
        VirtualSensorList sensorList = new VirtualSensorList();
        sensorList.sensors = new ArrayList<>();

        String req = baseStringForRequests +
                "SELECT ?sensor ?topic ?url ?interfaceType ?interfaceDescription ?longitude ?latitude ?alt ?altRelative ?relativeLocation\n" +
                "WHERE {\n" +
                "?sensor rdf:type ssn:Sensor .\n" +
                "?sensor ssn:madeObservation ?o .\n" +
                "?o ssn:observedProperty ?topic .\n" +
                "?sensor iot-lite:exposedBy ?s .\n" +
                "?topic qoo:canBeRetrievedThrough ?s .\n" +
                "?s iot-lite:endpoint ?url .\n" +
                "?s iot-lite:interfaceType ?interfaceType .\n" +
                "?s iot-lite:interfaceDescription ?interfaceDescription .\n" +
                "?sensor geo:location ?pos .\n" +
                "?pos geo:long ?longitude .\n" +
                "?pos geo:lat ?latitude .\n" +
                "?pos iot-lite:relativeLocation ?relativeLocation .\n" +
                "?pos iot-lite:altRelative ?altRelative .\n" +
                "?pos geo:alt ?alt\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();

                Resource id = (Resource) binding.get("sensor");
                Resource topic = (Resource) binding.get("topic");
                Literal url = binding.getLiteral("url");
                Literal interfaceType = binding.getLiteral("interfaceType");
                Literal interfaceDescription = binding.getLiteral("interfaceDescription");
                Literal longitude = binding.getLiteral("longitude");
                Literal latitude = binding.getLiteral("latitude");
                Literal alt = binding.getLiteral("alt");
                Literal altRelative = binding.getLiteral("altRelative");
                Literal relativeLocation = binding.getLiteral("relativeLocation");

                VirtualSensor sensorTemp;

                if (processedSensors.containsKey(id.getURI())) {
                    sensorTemp = processedSensors.get(id.getURI());
                }
                else {
                    sensorTemp = new VirtualSensor();
                    sensorTemp.sensor_id = id.getURI();

                    Location locTemp = new Location();
                    locTemp.latitude = latitude.getString();
                    locTemp.longitude = longitude.getString();
                    locTemp.altitude = alt.getString();
                    locTemp.relative_altitude = altRelative.getString();
                    locTemp.relative_location = relativeLocation.getString();

                    sensorTemp.endpoints = new ArrayList<>();
                    sensorTemp.location = locTemp;
                }

                ServiceEndpoint endpointTemp = new ServiceEndpoint();
                endpointTemp.topic = topic.getURI();
                endpointTemp.url = url.getString();
                endpointTemp.if_type = interfaceType.getString();
                endpointTemp.description = interfaceDescription.getString();
                sensorTemp.endpoints.add(endpointTemp);

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

    public VirtualSensor _findSpecificSensor(String sensor_id) {
        QuerySolution binding = null;
        VirtualSensor sensor = null;

        String req = baseStringForRequests +
                "SELECT ?sensor ?topic ?url ?interfaceType ?interfaceDescription ?longitude ?latitude ?alt ?altRelative ?relativeLocation\n" +
                "WHERE {\n" +
                "?sensor iot-lite:id \"" + sensor_id + "\" .\n" +
                "?sensor ssn:madeObservation ?o .\n" +
                "?o ssn:observedProperty ?topic .\n" +
                "?sensor iot-lite:exposedBy ?s .\n" +
                "?topic qoo:canBeRetrievedThrough ?s .\n" +
                "?s iot-lite:endpoint ?url .\n" +
                "?s iot-lite:interfaceType ?interfaceType .\n" +
                "?s iot-lite:interfaceDescription ?interfaceDescription .\n" +
                "?sensor geo:location ?pos .\n" +
                "?pos geo:long ?longitude .\n" +
                "?pos geo:lat ?latitude .\n" +
                "?pos iot-lite:relativeLocation ?relativeLocation .\n" +
                "?pos iot-lite:altRelative ?altRelative .\n" +
                "?pos geo:alt ?alt\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();

                Resource id = (Resource) binding.get("sensor");
                Resource topic = (Resource) binding.get("topic");
                Literal url = binding.getLiteral("url");
                Literal interfaceType = binding.getLiteral("interfaceType");
                Literal interfaceDescription = binding.getLiteral("interfaceDescription");
                Literal longitude = binding.getLiteral("longitude");
                Literal latitude = binding.getLiteral("latitude");
                Literal alt = binding.getLiteral("alt");
                Literal altRelative = binding.getLiteral("altRelative");
                Literal relativeLocation = binding.getLiteral("relativeLocation");

                if (sensor == null) {
                    sensor = new VirtualSensor();
                    sensor.sensor_id = id.getURI();

                    Location locTemp = new Location();
                    locTemp.latitude = latitude.getString();
                    locTemp.longitude = longitude.getString();
                    locTemp.altitude = alt.getString();
                    locTemp.relative_altitude = altRelative.getString();
                    locTemp.relative_location = relativeLocation.getString();
                    sensor.location = locTemp;

                    sensor.endpoints = new ArrayList<>();
                }

                ServiceEndpoint endpointTemp = new ServiceEndpoint();
                endpointTemp.topic = topic.getURI();
                endpointTemp.url = url.getString();
                endpointTemp.if_type = interfaceType.getString();
                endpointTemp.description = interfaceDescription.getString();
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

    public VirtualSensorList _findAllSensorsWithConditions(String locationNearTo, String topic_id) {
        Map<String, VirtualSensor> processedSensors = new ConcurrentHashMap<>();
        QuerySolution binding;
        VirtualSensorList sensorList = new VirtualSensorList();
        sensorList.sensors = new ArrayList<>();

        String req = baseStringForRequests +
                "SELECT ?sensor ?topic ?url ?interfaceType ?interfaceDescription ?longitude ?latitude ?alt ?altRelative ?relativeLocation\n" +
                "WHERE {\n" +
                "?sensor rdf:type ssn:Sensor .\n" +
                "?sensor ssn:madeObservation ?o .\n" +
                "?o ssn:observedProperty ?topic .\n" +
                "?sensor iot-lite:exposedBy ?s .\n";

        if (!topic_id.equals("ALL")) {
            req += "qoo:" + topic_id + " rdf:type ssn:Property .\n" +
                    "qoo:" + topic_id + " qoo:canBeRetrievedThrough ?s .\n";
        }

        req += "?topic qoo:canBeRetrievedThrough ?s .\n" +
                "?s iot-lite:endpoint ?url .\n" +
                "?s iot-lite:interfaceType ?interfaceType .\n" +
                "?s iot-lite:interfaceDescription ?interfaceDescription .\n" +
                "?area rdf:type geo:Point .\n";

        if (!locationNearTo.equals("ALL")) {
            req += "?area iot-lite:relativeLocation \"" + locationNearTo + "\" .\n";
        }

        req += "?sensor geo:location ?pos .\n" +
                "?pos geo:long ?longitude .\n" +
                "?pos geo:lat ?latitude .\n" +
                "?pos iot-lite:relativeLocation ?relativeLocation .\n" +
                "?pos iot-lite:altRelative ?altRelative .\n" +
                "?pos geo:alt ?alt\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();

                Resource id = (Resource) binding.get("sensor");
                Resource topic = (Resource) binding.get("topic");
                Literal url = binding.getLiteral("url");
                Literal interfaceType = binding.getLiteral("interfaceType");
                Literal interfaceDescription = binding.getLiteral("interfaceDescription");
                Literal longitude = binding.getLiteral("longitude");
                Literal latitude = binding.getLiteral("latitude");
                Literal alt = binding.getLiteral("alt");
                Literal altRelative = binding.getLiteral("altRelative");
                Literal relativeLocation = binding.getLiteral("relativeLocation");

                VirtualSensor sensorTemp;

                if (processedSensors.containsKey(id.getURI())) {
                    sensorTemp = processedSensors.get(id.getURI());
                }
                else {
                    sensorTemp = new VirtualSensor();
                    sensorTemp.sensor_id = id.getURI();

                    Location locTemp = new Location();
                    locTemp.latitude = latitude.getString();
                    locTemp.longitude = longitude.getString();
                    locTemp.altitude = alt.getString();
                    locTemp.relative_altitude = altRelative.getString();
                    locTemp.relative_location = relativeLocation.getString();

                    sensorTemp.endpoints = new ArrayList<>();
                    sensorTemp.location = locTemp;
                }

                ServiceEndpoint endpointTemp = new ServiceEndpoint();
                endpointTemp.topic = topic.getURI();
                endpointTemp.url = url.getString();
                endpointTemp.if_type = interfaceType.getString();
                endpointTemp.description = interfaceDescription.getString();
                sensorTemp.endpoints.add(endpointTemp);

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

    /**
     * Topics
     */

    public TopicList _findAllTopics() {
        QuerySolution binding;
        TopicList topicList = new TopicList();
        topicList.topics = new ArrayList<>();

        String req = baseStringForRequests +
                "SELECT DISTINCT ?topic\n" +
                "WHERE {\n" +
                " ?s rdf:type iot-lite:Service .\n" +
                " ?topic qoo:canBeRetrievedThrough ?s .\n" +
                " ?topic rdf:type ssn:Property .\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();
                Resource topic = (Resource) binding.get("topic");

                Topic topicTemp = new Topic();
                topicTemp.topic = topic.getURI();
                topicList.topics.add(topicTemp);
            }
        }

        if (topicList.topics.size() == 0) {
            return null;
        }
        else {
            return topicList;
        }
    }

    public ServiceEndpointList _findSpecificTopic(String topic_id) {
        QuerySolution binding = null;
        ServiceEndpointList endpointsForTopic = new ServiceEndpointList();
        endpointsForTopic.serviceEndpoints = new ArrayList<>();

        String req = baseStringForRequests +
                "SELECT ?topic ?service ?url ?interfaceType ?interfaceDescription\n" +
                "WHERE {\n" +
                "qoo:" + topic_id + " rdf:type ssn:Property .\n" +
                "qoo:" + topic_id + " qoo:canBeRetrievedThrough ?service .\n" +
                "?service iot-lite:endpoint ?url .\n" +
                "?service iot-lite:interfaceType ?interfaceType .\n" +
                "?service iot-lite:interfaceDescription ?interfaceDescription\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();
                Resource topicRes = (Resource) binding.get("service");
                Literal url = binding.getLiteral("url");
                Literal interfaceType = binding.getLiteral("interfaceType");
                Literal interfaceDescription = binding.getLiteral("interfaceDescription");

                ServiceEndpoint serviceEndpointTemp = new ServiceEndpoint();
                serviceEndpointTemp.topic = topicRes.getURI();
                serviceEndpointTemp.url = url.getString();
                serviceEndpointTemp.if_type = interfaceType.getString();
                serviceEndpointTemp.description = interfaceDescription.getString();

                endpointsForTopic.serviceEndpoints.add(serviceEndpointTemp);
            }
        }

        if (binding == null) {
            return null;
        }
        else {
            return endpointsForTopic;
        }
    }

    /**
     * Places
     */

    public LocationList _findAllPlaces() {
        QuerySolution binding = null;
        LocationList locationList = new LocationList();
        locationList.locations = new ArrayList<>();

        String req = baseStringForRequests +
                "SELECT DISTINCT ?latitude ?longitude ?altitude ?relativeLocation ?altRelative\n" +
                "WHERE {\n" +
                "  ?s rdf:type ssn:Sensor .\n" +
                "  ?s geo:location ?loc .\n" +
                "  ?loc rdf:type geo:Point .\n" +
                "  ?loc geo:lat ?latitude .\n" +
                "  ?loc geo:long ?longitude .\n" +
                "  ?loc geo:alt ?altitude .\n" +
                "  ?loc iot-lite:relativeLocation ?relativeLocation .\n" +
                "  ?loc iot-lite:altRelative ?altRelative \n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();
                Literal latitude = binding.getLiteral("latitude");
                Literal longitude = binding.getLiteral("longitude");
                Literal altitude = binding.getLiteral("altitude");
                Literal relative_altitude = binding.getLiteral("altRelative");
                Literal relative_location = binding.getLiteral("relativeLocation");

                Location locationTemp = new Location();
                locationTemp.latitude = latitude.getString();
                locationTemp.longitude = longitude.getString();
                locationTemp.altitude = altitude.getString();
                locationTemp.relative_altitude = relative_altitude.getString();
                locationTemp.relative_location = relative_location.getString();

                locationList.locations.add(locationTemp);
            }
        }

        if (binding == null) {
            return null;
        }
        else {
            return locationList;
        }
    }

    public LocationList _findPlacesNearTo(String location) {
        QuerySolution binding = null;
        LocationList locationList = new LocationList();
        locationList.locations = new ArrayList<>();

        String req = baseStringForRequests +
                "SELECT DISTINCT ?latitude ?longitude ?altitude ?relativeLocation ?altRelative\n" +
                "WHERE {\n" +
                "  ?s rdf:type ssn:Sensor .\n" +
                "    ?area rdf:type geo:Point .\n" +
                "    ?area iot-lite:relativeLocation \"" + location + "\" .\n" +
                "  ?s geo:location ?loc .\n" +
                "  ?loc rdf:type geo:Point .\n" +
                "  ?loc qoo:isInTheAreaOf ?area .\n" +
                "  ?loc geo:lat ?latitude .\n" +
                "  ?loc geo:long ?longitude .\n" +
                "  ?loc geo:alt ?altitude .\n" +
                "  ?loc iot-lite:relativeLocation ?relativeLocation .\n" +
                "  ?loc iot-lite:altRelative ?altRelative \n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();
                Literal latitude = binding.getLiteral("latitude");
                Literal longitude = binding.getLiteral("longitude");
                Literal altitude = binding.getLiteral("altitude");
                Literal relative_altitude = binding.getLiteral("altRelative");
                Literal relative_location = binding.getLiteral("relativeLocation");

                Location locationTemp = new Location();
                locationTemp.latitude = latitude.getString();
                locationTemp.longitude = longitude.getString();
                locationTemp.altitude = altitude.getString();
                locationTemp.relative_altitude = relative_altitude.getString();
                locationTemp.relative_location = relative_location.getString();

                locationList.locations.add(locationTemp);
            }
        }

        if (binding == null) {
            return null;
        }
        else {
            return locationList;
        }
    }

    /**
     * QoO attributes
     */

    public QoOAttributeList _findAllQoOAttributes() {
        QuerySolution binding = null;
        QoOAttributeList qoOAttributeList = new QoOAttributeList();
        qoOAttributeList.qoo_attributes = new ArrayList<>();

        String req = baseStringForRequests +
                "SELECT DISTINCT ?attribute ?variation\n" +
                "WHERE {\n" +
                "?attribute rdf:type qoo:QoOAttribute .\n" +
                "?attribute qoo:shouldBe ?variation\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();

                Resource attribute = (Resource) binding.get("attribute");
                Literal variation = binding.getLiteral("variation");

                QoOAttribute attrTemp = new QoOAttribute();
                attrTemp.name = attribute.getURI().split("#")[1];
                attrTemp.should_be = variation.getString();

                qoOAttributeList.qoo_attributes.add(attrTemp);
            }
        }

        if (binding == null) {
            return null;
        }
        else {
            return qoOAttributeList;
        }
    }

    /**
     * QoO customizable parameters
     */

    public QoOCustomizableParamList _findAllQoOCustomizableParameters() {
        QuerySolution binding = null;
        QoOCustomizableParamList customizableParamList = new QoOCustomizableParamList();
        customizableParamList.customizable_params = new ArrayList<>();

        String req = baseStringForRequests +
                "SELECT ?param ?doc\n" +
                "WHERE {\n" +
                "?param rdf:type qoo:QoOCustomizableParameter .\n" +
                "?param qoo:documentation ?doc\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();

                Resource param = (Resource) binding.get("param");
                String paramName = param.getURI().split("#")[1];
                Literal doc = binding.getLiteral("doc");

                QoOCustomizableParam paramTemp = new QoOCustomizableParam();
                paramTemp.param_name = paramName;
                paramTemp.details = doc.getString();

                customizableParamList.customizable_params.add(paramTemp);
            }
        }

        if (binding == null) {
            return null;
        }
        else {
            return customizableParamList;
        }
    }

}
