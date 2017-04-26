package fr.isae.iqas.database;

import fr.isae.iqas.config.Config;
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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 25/01/2017.
 */
// TODO to revise after ontology changes
public class FusekiController {
    private Logger log = LoggerFactory.getLogger(FusekiController.class);

    private String sparqlService;
    private String baseStringForRequests;
    private Map<String, String> namespaces;

    public FusekiController(Config iqasConfig) {
        this.sparqlService = iqasConfig.getProp().getProperty("fuseki_query_endpoint");

        this.namespaces = new ConcurrentHashMap<>();
        this.baseStringForRequests = "";
        for (String prefix : iqasConfig.getAllPrefixes()) {
            this.namespaces.put(prefix, iqasConfig.getIRIForPrefix(prefix, true));
            this.baseStringForRequests = this.baseStringForRequests.concat("PREFIX " + prefix + ": <" + namespaces.get(prefix) + ">\n");
        }

        log.info("FusekiController successfully created!");
    }

    /**
     * Sensors
     */

    public VirtualSensorList _findAllSensors() {
        QuerySolution binding;
        VirtualSensorList sensorList = new VirtualSensorList();
        sensorList.sensors = new ArrayList<>();

        final String req = baseStringForRequests +
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

        QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req);
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

            VirtualSensor sensorTemp = new VirtualSensor();

            sensorTemp.sensor_id = id.getURI();
            Location locTemp = new Location();
            locTemp.latitude = latitude.getString();
            locTemp.longitude = longitude.getString();
            locTemp.altitude = alt.getString();
            locTemp.relative_altitude = altRelative.getString();
            locTemp.relative_location = relativeLocation.getString();
            sensorTemp.location = locTemp;

            sensorTemp.endpoint = new ServiceEndpoint();
            sensorTemp.endpoint.topic = topic.getURI();
            sensorTemp.endpoint.url = url.getString();
            sensorTemp.endpoint.if_type = interfaceType.getString();
            sensorTemp.endpoint.description = interfaceDescription.getString();

            sensorList.sensors.add(sensorTemp);
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

        final String req = baseStringForRequests +
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

        QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req);
        ResultSet r = q.execSelect();
        if (r.hasNext()) {
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

            sensor = new VirtualSensor();
            sensor.sensor_id = id.getURI();

            Location locTemp = new Location();
            locTemp.latitude = latitude.getString();
            locTemp.longitude = longitude.getString();
            locTemp.altitude = alt.getString();
            locTemp.relative_altitude = altRelative.getString();
            locTemp.relative_location = relativeLocation.getString();
            sensor.location = locTemp;

            sensor.endpoint = new ServiceEndpoint();
            sensor.endpoint.topic = topic.getURI();
            sensor.endpoint.url = url.getString();
            sensor.endpoint.if_type = interfaceType.getString();
            sensor.endpoint.description = interfaceDescription.getString();
        }

        if (binding == null) {
            return null;
        }
        else {
            return sensor;
        }
    }

    public VirtualSensorList _findAllSensorsWithConditions(String locationNearTo, String topic_id) {
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

        QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req);
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

            VirtualSensor sensorTemp = new VirtualSensor();

            sensorTemp.sensor_id = id.getURI();
            Location locTemp = new Location();
            locTemp.latitude = latitude.getString();
            locTemp.longitude = longitude.getString();
            locTemp.altitude = alt.getString();
            locTemp.relative_altitude = altRelative.getString();
            locTemp.relative_location = relativeLocation.getString();

            sensorTemp.endpoint = new ServiceEndpoint();
            sensorTemp.location = locTemp;
            sensorTemp.endpoint.topic = topic.getURI();
            sensorTemp.endpoint.url = url.getString();
            sensorTemp.endpoint.if_type = interfaceType.getString();
            sensorTemp.endpoint.description = interfaceDescription.getString();

            sensorList.sensors.add(sensorTemp);
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

        final String req = baseStringForRequests +
                "SELECT DISTINCT ?topic\n" +
                "WHERE {\n" +
                " ?s rdf:type iot-lite:Service .\n" +
                " ?topic qoo:canBeRetrievedThrough ?s .\n" +
                " ?topic rdf:type ssn:Property .\n" +
                "}";

        QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req);
        ResultSet r = q.execSelect();
        while (r.hasNext()) {
            binding = r.nextSolution();
            Resource topic = (Resource) binding.get("topic");

            Topic topicTemp = new Topic();
            topicTemp.topic = topic.getURI();
            topicList.topics.add(topicTemp);
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

        final String req = baseStringForRequests +
                "SELECT ?topic ?service ?url ?interfaceType ?interfaceDescription\n" +
                "WHERE {\n" +
                "qoo:" + topic_id + " rdf:type ssn:Property .\n" +
                "qoo:" + topic_id + " qoo:canBeRetrievedThrough ?service .\n" +
                "?service iot-lite:endpoint ?url .\n" +
                "?service iot-lite:interfaceType ?interfaceType .\n" +
                "?service iot-lite:interfaceDescription ?interfaceDescription\n" +
                "}";

        QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req);
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

        final String req = baseStringForRequests +
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

        QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req);
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

        final String req = baseStringForRequests +
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

        QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req);
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

        final String req = baseStringForRequests +
                "SELECT DISTINCT ?attribute ?variation\n" +
                "WHERE {\n" +
                "?attribute rdf:type qoo:QoOAttribute .\n" +
                "?attribute qoo:shouldBe ?variation\n" +
                "}";

        QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req);
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
        Map<String, QoOCustomizableParam> processedParams = new ConcurrentHashMap<>();

        QuerySolution binding = null;
        QoOCustomizableParamList customizableParamList = new QoOCustomizableParamList();
        customizableParamList.customizable_params = new ArrayList<>();

        final String req = baseStringForRequests +
                "SELECT ?param ?doc ?impact ?capaVariation ?attrVariation\n" +
                "WHERE {\n" +
                "?param rdf:type qoo:QoOCustomizableParameter .\n" +
                "?param qoo:documentation ?doc .\n" +
                "?param qoo:has ?effect .\n" +
                "?effect qoo:impacts ?impact .\n" +
                "?effect qoo:capabilityVariation ?capaVariation .\n" +
                "?effect qoo:qooAttributeVariation ?attrVariation\n" +
                "}";

        QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req);
        ResultSet r = q.execSelect();
        while (r.hasNext()) {
            binding = r.nextSolution();

            Resource param = (Resource) binding.get("param");
            String paramName = param.getURI().split("#")[1];
            Literal doc = binding.getLiteral("doc");

            Resource impact = (Resource) binding.get("impact");
            String impactName = impact.getURI().split("#")[1];
            Literal capaVariation = binding.getLiteral("capaVariation");
            Literal attrVariation = binding.getLiteral("attrVariation");

            QoOCustomizableParam paramTemp;
            if (!processedParams.containsKey(paramName)) {
                paramTemp = new QoOCustomizableParam();
                paramTemp.param_name = paramName;
                paramTemp.details = doc.getString();
                paramTemp.has = new ArrayList<>();
            }
            else {
                paramTemp = processedParams.get(paramName);
            }
            QoOEffect qoOEffect = new QoOEffect();
            qoOEffect.impacts = impactName;
            qoOEffect.capabilityVariation = capaVariation.getString();
            qoOEffect.qooAttributeVariation = attrVariation.getString();
            paramTemp.has.add(qoOEffect);

            processedParams.put(paramName, paramTemp);
        }

        processedParams.forEach((k, v) -> {
            customizableParamList.customizable_params.add(v);
        });

        if (binding == null) {
            return null;
        }
        else {
            return customizableParamList;
        }
    }

    /**
     * Sensor capabilities
     */

    public SensorCapabilityList _findAllSensorCapabilities() {
        QuerySolution binding = null;
        SensorCapabilityList sensorCapabilityList = new SensorCapabilityList();
        sensorCapabilityList.sensorCapabilities = new ArrayList<>();

        final String req = baseStringForRequests +
                "SELECT ?sensor ?topic ?minValue ?maxValue\n" +
                "WHERE {\n" +
                "?sensor rdf:type ssn:Sensor .\n" +
                "?sensor ssn:hasMeasurementCapability ?capa .\n" +
                "?capa ssn:forProperty ?topic .\n" +
                "?capa ssn:hasMeasurementProperty ?prop .\n" +
                "?prop qoo:hasMinValue ?minValue .\n" +
                "?prop qoo:hasMaxValue ?maxValue\n" +
                "}";

        QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req);
        ResultSet r = q.execSelect();
        while (r.hasNext()) {
            binding = r.nextSolution();

            Resource param = (Resource) binding.get("sensor");
            String sensor_id = param.getURI().split("#")[1];
            param = (Resource) binding.get("topic");
            String topic = param.getURI().split("#")[1];
            String minValue = binding.getLiteral("minValue").getString();
            String maxValue = binding.getLiteral("maxValue").getString();

            if (minValue.equals("+INF")) {
                minValue = String.valueOf(Float.MAX_VALUE);
            }
            else if (minValue.equals("-INF")) {
                minValue = String.valueOf(Float.MIN_VALUE);
            }

            if (maxValue.equals("+INF")) {
                maxValue = String.valueOf(Float.MAX_VALUE);
            }
            else if (maxValue.equals("-INF")) {
                maxValue = String.valueOf(Float.MIN_VALUE);
            }

            SensorCapability paramTemp = new SensorCapability();
            paramTemp.sensor_id = sensor_id;
            paramTemp.topic = topic;
            paramTemp.min_value = minValue;
            paramTemp.max_value = maxValue;

            sensorCapabilityList.sensorCapabilities.add(paramTemp);
        }

        if (binding == null) {
            return null;
        }
        else {
            return sensorCapabilityList;
        }
    }

}
