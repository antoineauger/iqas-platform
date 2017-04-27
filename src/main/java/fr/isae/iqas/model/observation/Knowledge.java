package fr.isae.iqas.model.observation;

import fr.isae.iqas.model.jsonld.VirtualSensor;
import org.apache.jena.ontology.Individual;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;

import java.util.Map;

import static org.apache.jena.ontology.OntModelSpec.OWL_MEM;

/**
 * Created by an.auger on 08/02/2017.
 */
public class Knowledge extends RawData {
    private OntModel obsModel;

    public Knowledge(RawData rawData,
                     VirtualSensor virtualSensor,
                     Map<String, String> pref,
                     Map<String, OntClass> qooC,
                     Map<String, Property> qooP,
                     OntModel baseQoO) {

        super(rawData);
        this.obsModel = ModelFactory.createOntologyModel(OWL_MEM);

        // Observation and Sensor
        Individual obs = obsModel.createIndividual( pref.get("qoo") + "obs", qooC.get("ssn:Observation") );
        Individual sensor = obsModel.createIndividual( pref.get("qoo") + rawData.getProducer(), qooC.get("ssn:Sensor") );
        obs.setPropertyValue(qooP.get("ssn:observedBy"), sensor);

        // Location
        Individual locSensor = obsModel.createIndividual( pref.get("qoo") + "location", qooC.get("geo:Point") );
        locSensor.setPropertyValue(qooP.get("geo:lat"), baseQoO.createLiteral(virtualSensor.location.latitude));
        locSensor.setPropertyValue(qooP.get("geo:long"), baseQoO.createLiteral(virtualSensor.location.longitude));
        locSensor.setPropertyValue(qooP.get("geo:alt"), baseQoO.createLiteral(virtualSensor.location.altitude));
        locSensor.setPropertyValue(qooP.get("iot-lite:relativeLocation"), baseQoO.createLiteral(virtualSensor.location.relative_location));
        locSensor.setPropertyValue(qooP.get("iot-lite:altRelative"), baseQoO.createLiteral(virtualSensor.location.relative_altitude));
        sensor.setPropertyValue(qooP.get("geo:location"), locSensor);

        // TODO
        obs.setPropertyValue(qooP.get("ssn:featureOfInterest"), baseQoO.createLiteral(virtualSensor.madeObservation.featureOfInterest));
        obs.setPropertyValue(qooP.get("ssn:observedProperty"), baseQoO.createLiteral(virtualSensor.madeObservation.observedProperty));


        /*Individual propSensor = obsModel.createIndividual( pref.get("qoo") + "temperature", property );
        Individual sensorOutputInd = obsModel.createIndividual( qooIRI + "obsOutput", sensorOutput );
        Individual obsValueInd = obsModel.createIndividual( qooIRI + rawData.getValue(), obsValue );*/

    }

    public OntModel getObsModel() {
        return obsModel;
    }
}
