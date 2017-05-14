package fr.isae.iqas.database;

import fr.isae.iqas.config.Config;
import fr.isae.iqas.model.jsonld.*;
import fr.isae.iqas.utils.QualityUtils;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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

    VirtualSensorList _findAllSensors() {
        QuerySolution binding;
        VirtualSensorList sensorList = new VirtualSensorList();
        sensorList.sensors = new ArrayList<>();

        final String req = baseStringForRequests +
                "SELECT ?sensor ?topic ?fof ?unit ?quantityKind ?url ?interfaceType ?interfaceDescription ?longitude ?latitude ?alt ?altRelative ?relativeLocation\n" +
                "WHERE {\n" +
                "  ?sensor rdf:type ssn:Sensor .\n" +
                "  ?sensor ssn:madeObservation ?o .\n" +
                "  ?o ssn:observationResult ?obsResult .\n" +
                "  ?obsResult iot-lite:hasValue ?obsVal .\n" +
                "  ?obsVal iot-lite:hasUnit ?u .\n" +
                "  ?u rdf:type ?unit .\n" +
                "  ?obsVal iot-lite:hasQuantityKind ?q .\n" +
                "  ?q rdf:type ?quantityKind .\n" +
                "  ?o ssn:observedProperty ?topic .\n" +
                "  ?topic ssn:isPropertyOf ?fof .\n" +
                "  ?sensor iot-lite:exposedBy ?s .\n" +
                "  ?prop qoo:canBeRetrievedThrough ?s .\n" +
                "  ?s iot-lite:endpoint ?url .\n" +
                "  ?s iot-lite:interfaceType ?interfaceType .\n" +
                "  ?s iot-lite:interfaceDescription ?interfaceDescription .\n" +
                "  ?sensor geo:location ?pos .\n" +
                "  ?pos geo:long ?longitude .\n" +
                "  ?pos geo:lat ?latitude .\n" +
                "  ?pos iot-lite:relativeLocation ?relativeLocation .\n" +
                "  ?pos iot-lite:altRelative ?altRelative .\n" +
                "  ?pos geo:alt ?alt\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();

                Resource id = (Resource) binding.get("sensor");
                Resource topic = (Resource) binding.get("topic");
                Resource fof = (Resource) binding.get("fof");
                Resource unit = (Resource) binding.get("unit");
                Resource quantityKind = (Resource) binding.get("quantityKind");
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

                sensorTemp.location = new Location();
                sensorTemp.location.latitude = latitude.getString();
                sensorTemp.location.longitude = longitude.getString();
                sensorTemp.location.altitude = alt.getString();
                sensorTemp.location.relative_altitude = altRelative.getString();
                sensorTemp.location.relative_location = relativeLocation.getString();

                sensorTemp.madeObservation = new Observation();
                sensorTemp.madeObservation.featureOfInterest = fof.getURI();
                sensorTemp.madeObservation.observedProperty = topic.getURI();

                sensorTemp.observationValue = new ObservationValue();
                sensorTemp.observationValue.hasQuantityKind = quantityKind.getURI();
                sensorTemp.observationValue.hasUnit = unit.getURI();

                sensorTemp.endpoint = new ServiceEndpoint();
                sensorTemp.endpoint.url = url.getString();
                sensorTemp.endpoint.if_type = interfaceType.getString();
                sensorTemp.endpoint.description = interfaceDescription.getString();

                sensorList.sensors.add(sensorTemp);
            }
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
        VirtualSensor sensor = null;

        final String req = baseStringForRequests +
                "SELECT ?sensor ?topic ?fof ?unit ?quantityKind ?url ?interfaceType ?interfaceDescription ?longitude ?latitude ?alt ?altRelative ?relativeLocation\n" +
                "WHERE {\n" +
                "  ?sensor iot-lite:id \"" + sensor_id + "\" .\n" +
                "  ?sensor rdf:type ssn:Sensor .\n" +
                "  ?sensor ssn:madeObservation ?o .\n" +
                "  ?o ssn:observationResult ?obsResult .\n" +
                "  ?obsResult iot-lite:hasValue ?obsVal .\n" +
                "  ?obsVal iot-lite:hasUnit ?u .\n" +
                "  ?u rdf:type ?unit .\n" +
                "  ?obsVal iot-lite:hasQuantityKind ?q .\n" +
                "  ?q rdf:type ?quantityKind .\n" +
                "  ?o ssn:observedProperty ?topic .\n" +
                "  ?topic ssn:isPropertyOf ?fof .\n" +
                "  ?sensor iot-lite:exposedBy ?s .\n" +
                "  ?prop qoo:canBeRetrievedThrough ?s .\n" +
                "  ?s iot-lite:endpoint ?url .\n" +
                "  ?s iot-lite:interfaceType ?interfaceType .\n" +
                "  ?s iot-lite:interfaceDescription ?interfaceDescription .\n" +
                "  ?sensor geo:location ?pos .\n" +
                "  ?pos geo:long ?longitude .\n" +
                "  ?pos geo:lat ?latitude .\n" +
                "  ?pos iot-lite:relativeLocation ?relativeLocation .\n" +
                "  ?pos iot-lite:altRelative ?altRelative .\n" +
                "  ?pos geo:alt ?alt\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            if (r.hasNext()) {
                binding = r.nextSolution();

                Resource id = (Resource) binding.get("sensor");
                Resource topic = (Resource) binding.get("topic");
                Resource fof = (Resource) binding.get("fof");
                Resource unit = (Resource) binding.get("unit");
                Resource quantityKind = (Resource) binding.get("quantityKind");
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

                sensor.location = new Location();
                sensor.location.latitude = latitude.getString();
                sensor.location.longitude = longitude.getString();
                sensor.location.altitude = alt.getString();
                sensor.location.relative_altitude = altRelative.getString();
                sensor.location.relative_location = relativeLocation.getString();

                sensor.madeObservation = new Observation();
                sensor.madeObservation.featureOfInterest = fof.getURI();
                sensor.madeObservation.observedProperty = topic.getURI();

                sensor.observationValue = new ObservationValue();
                sensor.observationValue.hasQuantityKind = quantityKind.getURI();
                sensor.observationValue.hasUnit = unit.getURI();

                sensor.endpoint = new ServiceEndpoint();
                sensor.endpoint.url = url.getString();
                sensor.endpoint.if_type = interfaceType.getString();
                sensor.endpoint.description = interfaceDescription.getString();
            }
        }

        if (binding == null) {
            return null;
        }
        else {
            return sensor;
        }
    }

    public VirtualSensorList findAllSensorsWithConditions(String locationNearTo, String topic_id) {
        QuerySolution binding;
        VirtualSensorList sensorList = new VirtualSensorList();
        sensorList.sensors = new ArrayList<>();

        String req = baseStringForRequests +
                "SELECT ?sensor ?topic ?fof ?unit ?quantityKind ?url ?interfaceType ?interfaceDescription ?longitude ?latitude ?alt ?altRelative ?relativeLocation\n" +
                "WHERE {\n" +
                "  ?sensor rdf:type ssn:Sensor .\n" +
                "  ?sensor ssn:madeObservation ?o .\n" +
                "  ?o ssn:observationResult ?obsResult .\n" +
                "  ?obsResult iot-lite:hasValue ?obsVal .\n" +
                "  ?obsVal iot-lite:hasUnit ?u .\n" +
                "  ?u rdf:type ?unit .\n" +
                "  ?obsVal iot-lite:hasQuantityKind ?q .\n" +
                "  ?q rdf:type ?quantityKind .\n" +
                "  ?o ssn:observedProperty ?topic .\n" +
                "  ?topic ssn:isPropertyOf ?fof .\n" +
                "  ?sensor iot-lite:exposedBy ?s .\n";

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
                Resource fof = (Resource) binding.get("fof");
                Resource unit = (Resource) binding.get("unit");
                Resource quantityKind = (Resource) binding.get("quantityKind");
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

                sensorTemp.location = new Location();
                sensorTemp.location.latitude = latitude.getString();
                sensorTemp.location.longitude = longitude.getString();
                sensorTemp.location.altitude = alt.getString();
                sensorTemp.location.relative_altitude = altRelative.getString();
                sensorTemp.location.relative_location = relativeLocation.getString();

                sensorTemp.madeObservation = new Observation();
                sensorTemp.madeObservation.featureOfInterest = fof.getURI();
                sensorTemp.madeObservation.observedProperty = topic.getURI();

                sensorTemp.observationValue = new ObservationValue();
                sensorTemp.observationValue.hasQuantityKind = quantityKind.getURI();
                sensorTemp.observationValue.hasUnit = unit.getURI();

                sensorTemp.endpoint = new ServiceEndpoint();
                sensorTemp.endpoint.url = url.getString();
                sensorTemp.endpoint.if_type = interfaceType.getString();
                sensorTemp.endpoint.description = interfaceDescription.getString();

                sensorList.sensors.add(sensorTemp);
            }
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

    public TopicList findAllTopics() {
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

    ServiceEndpointList _findSpecificTopic(String topic_id) {
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

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();
                Literal url = binding.getLiteral("url");
                Literal interfaceType = binding.getLiteral("interfaceType");
                Literal interfaceDescription = binding.getLiteral("interfaceDescription");

                ServiceEndpoint serviceEndpointTemp = new ServiceEndpoint();
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

    LocationList _findAllPlaces() {
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

    LocationList _findPlacesNearTo(String location) {
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

    QoOAttributeList _findAllQoOAttributes() {
        QuerySolution binding = null;
        QoOAttributeList qoOAttributeList = new QoOAttributeList();
        qoOAttributeList.qoo_attributes = new ArrayList<>();

        final String req = baseStringForRequests +
                "SELECT DISTINCT ?attribute ?variation\n" +
                "WHERE {\n" +
                "  ?attribute rdf:type qoo:QoOAttribute .\n" +
                "  ?attribute qoo:shouldBe ?variation\n" +
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

    QoOCustomizableParamList _findAllQoOCustomizableParameters() {
        Map<String, QoOCustomizableParam> processedParams = new ConcurrentHashMap<>();

        QuerySolution binding = null;
        QoOCustomizableParamList customizableParamList = new QoOCustomizableParamList();
        customizableParamList.customizable_params = new ArrayList<>();

        final String req = baseStringForRequests +
                "SELECT ?param ?doc ?impact ?paramVariation ?attrVariation\n" +
                "WHERE {\n" +
                "  ?param rdf:type qoo:QoOCustomizableParameter .\n" +
                "  ?param qoo:documentation ?doc .\n" +
                "  ?param qoo:has ?effect .\n" +
                "  ?effect qoo:impacts ?impact .\n" +
                "  ?effect qoo:paramVariation ?paramVariation .\n" +
                "  ?effect qoo:qooAttributeVariation ?attrVariation\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
            ResultSet r = q.execSelect();
            while (r.hasNext()) {
                binding = r.nextSolution();

                Resource param = (Resource) binding.get("param");
                String paramName = param.getURI().split("#")[1];
                Literal doc = binding.getLiteral("doc");

                Resource impact = (Resource) binding.get("impact");
                String impactName = impact.getURI().split("#")[1];
                Literal paramVariation = binding.getLiteral("paramVariation");
                Literal attrVariation = binding.getLiteral("attrVariation");

                QoOCustomizableParam paramTemp;
                if (!processedParams.containsKey(paramName)) {
                    paramTemp = new QoOCustomizableParam();
                    paramTemp.param_name = paramName;
                    paramTemp.documentation = doc.getString();
                    paramTemp.has = new ArrayList<>();
                } else {
                    paramTemp = processedParams.get(paramName);
                }
                QoOEffect qoOEffect = new QoOEffect();
                qoOEffect.impacts = impactName;
                qoOEffect.paramVariation = paramVariation.getString();
                qoOEffect.qooAttributeVariation = attrVariation.getString();
                paramTemp.has.add(qoOEffect);

                processedParams.put(paramName, paramTemp);
            }
        }

        if (binding == null) {
            return null;
        }
        else {
            customizableParamList.customizable_params.addAll(processedParams.values());
            return customizableParamList;
        }
    }

    /**
     * Sensor capabilities
     */

    public SensorCapabilityList findAllSensorCapabilities() {
        QuerySolution binding = null;
        SensorCapabilityList sensorCapabilityList = new SensorCapabilityList();
        sensorCapabilityList.sensorCapabilities = new ArrayList<>();

        final String req = baseStringForRequests +
                "SELECT ?sensor ?topic ?minValue ?maxValue\n" +
                "WHERE {\n" +
                "  ?sensor rdf:type ssn:Sensor .\n" +
                "  ?sensor ssn:hasMeasurementCapability ?capa .\n" +
                "  ?capa ssn:forProperty ?topic .\n" +
                "  ?capa ssn:hasMeasurementProperty ?prop .\n" +
                "  ?prop qoo:hasMinValue ?minValue .\n" +
                "  ?prop qoo:hasMaxValue ?maxValue\n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req)) {
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
                } else if (minValue.equals("-INF")) {
                    minValue = String.valueOf(Float.MIN_VALUE);
                }

                if (maxValue.equals("+INF")) {
                    maxValue = String.valueOf(Float.MAX_VALUE);
                } else if (maxValue.equals("-INF")) {
                    maxValue = String.valueOf(Float.MIN_VALUE);
                }

                SensorCapability paramTemp = new SensorCapability();
                paramTemp.sensor_id = sensor_id;
                paramTemp.topic = topic;
                paramTemp.min_value = minValue;
                paramTemp.max_value = maxValue;

                sensorCapabilityList.sensorCapabilities.add(paramTemp);
            }
        }

        if (binding == null) {
            return null;
        }
        else {
            return sensorCapabilityList;
        }
    }

    /**
     * QoOPipeline
     */

    public QoOPipelineList findMatchingPipelinesToHeal(fr.isae.iqas.model.quality.QoOAttribute priorityAttr, List<fr.isae.iqas.model.quality.QoOAttribute> attrToPreserve) {
        Map<String, QoOPipeline> pipelineMap = new ConcurrentHashMap<>();      // Pipeline_id <-> QoOPipeline
        Map<String, String> mappingMap = new ConcurrentHashMap<>();   // QoOCustomizableParam_id <-> Pipeline_id
        Map<String, QoOCustomizableParam> paramsMap = new ConcurrentHashMap<>();   // QoOCustomizableParam_id <-> QoOCustomizableParams

        QuerySolution binding1;
        QoOPipelineList qoOPipelineList = new QoOPipelineList();
        qoOPipelineList.qoOPipelines = new ArrayList<>();

        // Step 1: retrieval of all QoOAttributes and their 'shouldBe' property

        Map<fr.isae.iqas.model.quality.QoOAttribute, String> attrShouldNotBe = new ConcurrentHashMap<>();

        final String req1 = baseStringForRequests +
                "SELECT DISTINCT ?attr ?var\n" +
                "WHERE {\n" +
                "  ?attr rdf:type qoo:QoOAttribute .\n" +
                "  ?attr qoo:shouldBe ?var \n" +
                "}";

        try (QueryExecution q = QueryExecutionFactory.sparqlService(sparqlService, req1)) {
            ResultSet r1 = q.execSelect();
            while (r1.hasNext()) {
                binding1 = r1.nextSolution();

                Resource param = (Resource) binding1.get("attr");
                fr.isae.iqas.model.quality.QoOAttribute attr = fr.isae.iqas.model.quality.QoOAttribute.valueOf(param.getURI().split("#")[1]);
                String var = binding1.getLiteral("var").getString();
                attrShouldNotBe.put(attr, QualityUtils.inverseOfVariation(var));
            }
        }

        if (attrShouldNotBe.size() == 0) {
            return qoOPipelineList;
        }

        // Step 2: search for some matching QoOPipelines that meet parameters priorityAttr / attrToPreserve

        StringBuilder req2 = new StringBuilder(baseStringForRequests +
                "SELECT DISTINCT ?attr ?pipeline ?param ?paramType ?paramMinValue ?paramMaxValue ?paramInitialValue ?paramVariation ?attrVar\n" +
                "WHERE {   \n" +
                "  ?attr rdf:type qoo:QoOAttribute .\n" +
                "  FILTER (?attr = qoo:" + priorityAttr.toString() + ") .\n" +
                "  ?attr qoo:shouldBe ?attrVar .\n" +
                "  ?pipeline rdf:type qoo:QoOPipeline .\n" +
                "  ?pipeline qoo:allowsToSet ?param .\n" +
                "  ?param qoo:has ?impact .\n" +
                "  ?param qoo:paramType ?paramType .\n" +
                "  ?param qoo:paramMinValue ?paramMinValue .\n" +
                "  ?param qoo:paramMaxValue ?paramMaxValue .\n" +
                "  ?param qoo:paramInitialValue ?paramInitialValue .\n" +
                "  ?impact qoo:paramVariation ?paramVariation .\n" +
                "  ?impact qoo:qooAttributeVariation ?attrVar .\n" +
                "  ?impact qoo:impacts ?attr .\n");

        for (fr.isae.iqas.model.quality.QoOAttribute attr : attrToPreserve) {
            if (!attr.equals(priorityAttr)) {
                req2.append("FILTER (NOT EXISTS { \n" +
                        "      ?param qoo:has ?impact2 .\n" +
                        "      ?impact2 qoo:qooAttributeVariation \"" + attrShouldNotBe.get(attr) + "\" .\n" +
                        "      ?impact2 qoo:impacts ?attr2 .\n" +
                        "      FILTER (?attr2 = qoo:" + attr.toString() + ")\n" +
                        "    })\n");
            }
        }

        req2.append("}");

        QuerySolution binding2 = null;

        try (QueryExecution q2 = QueryExecutionFactory.sparqlService(sparqlService, req2.toString())) {
            ResultSet r2 = q2.execSelect();
            while (r2.hasNext()) {
                binding2 = r2.nextSolution();

                Resource pipeline = (Resource) binding2.get("pipeline");
                String pipeline_id = pipeline.getURI().split("#")[1];
                Resource param = (Resource) binding2.get("param");
                String param_name = param.getURI().split("#")[1];
                Resource attr = (Resource) binding2.get("attr");
                Literal paramVariation = binding2.getLiteral("paramVariation");
                Literal attrVar = binding2.getLiteral("attrVar");
                Literal paramType = binding2.getLiteral("paramType");
                Literal paramMinValue = binding2.getLiteral("paramMinValue");
                Literal paramMaxValue = binding2.getLiteral("paramMaxValue");
                Literal paramInitialValue = binding2.getLiteral("paramInitialValue");

                if (!pipelineMap.containsKey(pipeline_id)) {
                    QoOPipeline qoOPipeline = new QoOPipeline();
                    qoOPipeline.pipeline = pipeline_id;
                    qoOPipeline.customizable_params = new ArrayList<>();
                    pipelineMap.put(pipeline_id, qoOPipeline);
                }

                if (!paramsMap.containsKey(param_name)) {
                    QoOCustomizableParam qoOCustomizableParam = new QoOCustomizableParam();
                    qoOCustomizableParam.param_name = param_name;
                    qoOCustomizableParam.paramInitialValue = paramInitialValue.getString();
                    qoOCustomizableParam.paramType = paramType.getString();
                    qoOCustomizableParam.paramMinValue = paramMinValue.getString();
                    qoOCustomizableParam.paramMaxValue = paramMaxValue.getString();
                    qoOCustomizableParam.has = new ArrayList<>();
                    paramsMap.put(param_name, qoOCustomizableParam);
                }
                QoOCustomizableParam qoOCustomizableParamTemp = paramsMap.get(param_name);

                QoOEffect qoOEffectTemp = new QoOEffect();
                qoOEffectTemp.impacts = attr.getURI();
                qoOEffectTemp.qooAttributeVariation = attrVar.getString();
                qoOEffectTemp.paramVariation = paramVariation.getString();
                qoOCustomizableParamTemp.has.add(qoOEffectTemp);

                mappingMap.put(qoOCustomizableParamTemp.param_name, pipeline_id);
            }
        }

        mappingMap.forEach((key, val) -> {
            pipelineMap.get(val).customizable_params.add(paramsMap.get(key));
        });

        if (binding2 == null) {
            return null;
        }
        else {
            qoOPipelineList.qoOPipelines.addAll(pipelineMap.values());
            return qoOPipelineList;
        }
    }

}
