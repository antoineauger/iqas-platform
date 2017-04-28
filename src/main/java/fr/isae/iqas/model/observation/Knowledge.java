package fr.isae.iqas.model.observation;

import fr.isae.iqas.model.jsonld.VirtualSensor;
import fr.isae.iqas.model.quality.QoOAttribute;
import org.apache.jena.ontology.Individual;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import static fr.isae.iqas.model.quality.QoOAttribute.OBS_ACCURACY;
import static fr.isae.iqas.model.quality.QoOAttribute.OBS_FRESHNESS;
import static org.apache.jena.ontology.OntModelSpec.OWL_MEM;

/**
 * Created by an.auger on 08/02/2017.
 */
public class Knowledge extends RawData {
    private Map<String, String> pref;
    private Map<String, OntClass> qooC;
    private Map<String, Property> qooP;
    private OntModel obsModel;

    public Knowledge(RawData rawData,
                     VirtualSensor virtualSensor,
                     Map<String, String> pref,
                     Map<String, OntClass> qooC,
                     Map<String, Property> qooP,
                     OntModel qooBaseModel) {

        super(rawData);

        this.pref = pref;
        this.qooC = qooC;
        this.qooP = qooP;
        this.obsModel = ModelFactory.createOntologyModel(OWL_MEM);

        // Observation and Sensor
        Individual obsInd = obsModel.createIndividual( pref.get("qoo") + "obs", qooC.get("ssn:Observation") );
        Individual sensorInd = obsModel.createIndividual( pref.get("qoo") + rawData.getProducer(), qooC.get("ssn:Sensor") );
        obsInd.setPropertyValue(qooP.get("ssn:observedBy"), sensorInd);

        // FoI and Property
        obsInd.setPropertyValue(qooP.get("ssn:featureOfInterest"), obsModel.createLiteral(virtualSensor.madeObservation.featureOfInterest));
        obsInd.setPropertyValue(qooP.get("ssn:observedProperty"), obsModel.createLiteral(virtualSensor.madeObservation.observedProperty));

        // Location
        Individual locSensorInd = obsModel.createIndividual( pref.get("qoo") + "location", qooC.get("geo:Point") );
        locSensorInd.setPropertyValue(qooP.get("geo:lat"), obsModel.createLiteral(virtualSensor.location.latitude));
        locSensorInd.setPropertyValue(qooP.get("geo:long"), obsModel.createLiteral(virtualSensor.location.longitude));
        locSensorInd.setPropertyValue(qooP.get("geo:alt"), obsModel.createLiteral(virtualSensor.location.altitude));
        locSensorInd.setPropertyValue(qooP.get("iot-lite:relativeLocation"), obsModel.createLiteral(virtualSensor.location.relative_location));
        locSensorInd.setPropertyValue(qooP.get("iot-lite:altRelative"), obsModel.createLiteral(virtualSensor.location.relative_altitude));
        sensorInd.setPropertyValue(qooP.get("geo:location"), locSensorInd);

        // ObservationValue
        Individual sensorOutputInd = obsModel.createIndividual( pref.get("qoo") + "sensorOutput", qooC.get("ssn:SensorOutput") );
        Individual obsValueInd = obsModel.createIndividual( pref.get("qoo") + "obsValue", qooC.get("ssn:ObservationValue") );
        obsInd.setPropertyValue(qooP.get("ssn:observationResult"), sensorOutputInd);
        sensorOutputInd.setPropertyValue(qooP.get("ssn:hasValue"), obsValueInd);

        Individual qkInd = qooBaseModel.getIndividual(virtualSensor.observationValue.hasQuantityKind);
        Individual unitInd = qooBaseModel.getIndividual(virtualSensor.observationValue.hasUnit);
        obsValueInd.setPropertyValue(qooP.get("iot-lite:hasUnit"), unitInd);
        obsValueInd.setPropertyValue(qooP.get("iot-lite:hasQuantityKind"), qkInd);
        obsValueInd.setPropertyValue(qooP.get("qoo:obsStrValue"), obsModel.createLiteral(String.valueOf(rawData.getValue())));
        obsValueInd.setPropertyValue(qooP.get("qoo:levelValue"), obsModel.createLiteral("KNOWLEDGE"));

        // QoO
        obsModel.createSeq(pref.get("qoo") + "qooAttributesList");
    }

    @Override
    public String toString() {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        RDFDataMgr.write(os, obsModel, RDFFormat.JSONLD_COMPACT_PRETTY);
        try {
            JSONObject obsJSON = new JSONObject(new String(os.toByteArray(),"UTF-8"));
            JSONArray graphJSON = obsJSON.getJSONArray("@graph");
            return new JSONObject().put("obs", graphJSON).toString(2);
        } catch (UnsupportedEncodingException | JSONException e) {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * Getters and setters for a specific QoO attribute
     */

    @Override
    public void setQoOAttribute(String attribute, String value) {
        Individual obsValueInd = obsModel.getIndividual( pref.get("qoo") + "obsValue");
        Seq list = obsModel.getSeq(pref.get("qoo") + "qooAttributesList");

        Individual qooIntrinsicQualityInd = obsModel.createIndividual(pref.get("qoo") + "qooIntrinsic_" + attribute, qooC.get("qoo:QoOIntrisicQuality"));
        qooIntrinsicQualityInd.setPropertyValue(qooP.get("qoo:isAbout"), obsModel.createLiteral(attribute));
        Individual qooValueInd = obsModel.createIndividual( pref.get("qoo") + "qooValue_" + attribute, qooC.get("qoo:QoOValue") );

        Individual qooQkInd;
        Individual qooUnitInd;
        if (attribute.equals("OBS_FRESHNESS")) {
            qooUnitInd = obsModel.createIndividual( pref.get("qoo") + "freshnessUnit", qooC.get("m3-lite:Millisecond") );
            qooQkInd = obsModel.createIndividual( pref.get("qoo") + "freshnessKind", qooC.get("m3-lite:Others") );
            qooValueInd.setPropertyValue(qooP.get("iot-lite:hasUnit"), qooUnitInd);
            qooValueInd.setPropertyValue(qooP.get("iot-lite:hasQuantityKind"), qooQkInd);
        }
        else if (attribute.equals("OBS_ACCURACY")) {
            qooUnitInd = obsModel.createIndividual( pref.get("qoo") + "accuracyUnit", qooC.get("m3-lite:Percent") );
            qooQkInd = obsModel.createIndividual( pref.get("qoo") + "accuracyKind", qooC.get("m3-lite:Others") );
            qooValueInd.setPropertyValue(qooP.get("iot-lite:hasUnit"), qooUnitInd);
            qooValueInd.setPropertyValue(qooP.get("iot-lite:hasQuantityKind"), qooQkInd);
        }
        qooValueInd.setPropertyValue(qooP.get("qoo:qooStrValue"), obsModel.createLiteral(value));

        qooIntrinsicQualityInd.setPropertyValue(qooP.get("qoo:hasQoOValue"), qooValueInd);

        list.add(qooIntrinsicQualityInd);
        obsValueInd.setPropertyValue(qooP.get("qoo:hasQoO"), list);
    }

    @Override
    public String getQoOAttribute(String attribute) {
        return obsModel.getIndividual(pref.get("qoo") + "qooValue_" + attribute).getPropertyValue(qooP.get("qoo:qooStrValue")).toString();
    }

    @Override
    public void setQoOAttribute(QoOAttribute attribute, String value) {
        Individual obsValueInd = obsModel.getIndividual( pref.get("qoo") + "obsValue");
        Seq list = obsModel.getSeq(pref.get("qoo") + "qooAttributesList");

        Individual qooIntrinsicQualityInd = obsModel.createIndividual(pref.get("qoo") + "qooIntrinsic_" + attribute.toString(), qooC.get("qoo:QoOIntrisicQuality"));
        qooIntrinsicQualityInd.setPropertyValue(qooP.get("qoo:isAbout"), obsModel.createLiteral(attribute.toString()));
        Individual qooValueInd = obsModel.createIndividual( pref.get("qoo") + "qooValue_" + attribute.toString(), qooC.get("qoo:QoOValue") );

        Individual qooQkInd;
        Individual qooUnitInd;
        if (attribute.equals(OBS_FRESHNESS)) {
            qooUnitInd = obsModel.createIndividual( pref.get("qoo") + "freshnessUnit", qooC.get("m3-lite:Millisecond") );
            qooQkInd = obsModel.createIndividual( pref.get("qoo") + "freshnessKind", qooC.get("m3-lite:Others") );
            qooValueInd.setPropertyValue(qooP.get("iot-lite:hasUnit"), qooUnitInd);
            qooValueInd.setPropertyValue(qooP.get("iot-lite:hasQuantityKind"), qooQkInd);
        }
        else if (attribute.equals(OBS_ACCURACY)) {
            qooUnitInd = obsModel.createIndividual( pref.get("qoo") + "accuracyUnit", qooC.get("m3-lite:Percent") );
            qooQkInd = obsModel.createIndividual( pref.get("qoo") + "accuracyKind", qooC.get("m3-lite:Others") );
            qooValueInd.setPropertyValue(qooP.get("iot-lite:hasUnit"), qooUnitInd);
            qooValueInd.setPropertyValue(qooP.get("iot-lite:hasQuantityKind"), qooQkInd);
        }
        qooValueInd.setPropertyValue(qooP.get("qoo:qooStrValue"), obsModel.createLiteral(value));

        qooIntrinsicQualityInd.setPropertyValue(qooP.get("qoo:hasQoOValue"), qooValueInd);

        list.add(qooIntrinsicQualityInd);
        obsValueInd.setPropertyValue(qooP.get("qoo:hasQoO"), list);
    }

    @Override
    public String getQoOAttribute(QoOAttribute attribute) {
        return obsModel.getIndividual(pref.get("qoo") + "qooValue_" + attribute.toString()).getPropertyValue(qooP.get("qoo:qooStrValue")).toString();
    }
}
