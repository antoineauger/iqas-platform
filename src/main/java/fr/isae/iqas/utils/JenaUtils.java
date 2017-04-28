package fr.isae.iqas.utils;

import fr.isae.iqas.config.Config;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntDocumentManager;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;

import java.util.HashMap;
import java.util.Map;

import static org.apache.jena.ontology.OntModelSpec.OWL_MEM;

/**
 * Created by an.auger on 26/04/2017.
 */
public class JenaUtils {
    public static Map<String, String> getPrefixes(Config iqasConfig) {
        Map<String, String> allPrefixes = new HashMap<>();
        for (String prefix : iqasConfig.getAllPrefixes()) {
            allPrefixes.put(prefix, iqasConfig.getIRIForPrefix(prefix, true));
        }
        return allPrefixes;
    }

    public static Map<String, Property> getUsefulProperties(Config iqasConfig, OntModel qooBaseModel) {
        Map<String, Property> allUsefulProperties = new HashMap<>();
        String ssnIRI = iqasConfig.getIRIForPrefix("ssn", true);
        String geoIRI = iqasConfig.getIRIForPrefix("geo", true);
        String iotliteIRI = iqasConfig.getIRIForPrefix("iot-lite", true);
        String qooIRI = iqasConfig.getIRIForPrefix("qoo", true);
        allUsefulProperties.put("ssn:observedBy", qooBaseModel.getProperty(ssnIRI + "observedBy"));
        allUsefulProperties.put("ssn:featureOfInterest", qooBaseModel.getProperty(ssnIRI + "featureOfInterest"));
        allUsefulProperties.put("ssn:observedProperty", qooBaseModel.getProperty(ssnIRI + "observedProperty"));
        allUsefulProperties.put("geo:lat", qooBaseModel.getProperty(geoIRI + "lat"));
        allUsefulProperties.put("geo:long", qooBaseModel.getProperty(geoIRI + "long"));
        allUsefulProperties.put("geo:alt", qooBaseModel.getProperty(geoIRI + "alt"));
        allUsefulProperties.put("geo:location", qooBaseModel.getProperty(geoIRI + "location"));
        allUsefulProperties.put("iot-lite:relativeLocation", qooBaseModel.getProperty(iotliteIRI + "relativeLocation"));
        allUsefulProperties.put("iot-lite:altRelative", qooBaseModel.getProperty(iotliteIRI + "altRelative"));
        allUsefulProperties.put("ssn:observationResult", qooBaseModel.getProperty(ssnIRI + "observationResult"));
        allUsefulProperties.put("ssn:hasValue", qooBaseModel.getProperty(ssnIRI + "hasValue"));
        allUsefulProperties.put("iot-lite:hasUnit", qooBaseModel.getProperty(iotliteIRI + "hasUnit"));
        allUsefulProperties.put("iot-lite:hasQuantityKind", qooBaseModel.getProperty(iotliteIRI + "hasQuantityKind"));
        allUsefulProperties.put("qoo:obsStrValue", qooBaseModel.getProperty(qooIRI + "obsStrValue"));
        allUsefulProperties.put("qoo:obsLevelValue", qooBaseModel.getProperty(qooIRI + "obsLevelValue"));
        allUsefulProperties.put("qoo:obsTimestampsValue", qooBaseModel.getProperty(qooIRI + "obsTimestampsValue"));
        allUsefulProperties.put("qoo:obsDateValue", qooBaseModel.getProperty(qooIRI + "obsDateValue"));
        allUsefulProperties.put("qoo:hasQoO", qooBaseModel.getProperty(qooIRI + "hasQoO"));
        allUsefulProperties.put("qoo:qooStrValue", qooBaseModel.getProperty(qooIRI + "qooStrValue"));
        allUsefulProperties.put("qoo:hasQoOValue", qooBaseModel.getProperty(qooIRI + "hasQoOValue"));
        allUsefulProperties.put("qoo:isAbout", qooBaseModel.getProperty(qooIRI + "isAbout"));
        return allUsefulProperties;
    }

    public static Map<String, OntClass> getUsefulOntClasses(Config iqasConfig, OntModel qooBaseModel) {
        Map<String, OntClass> allUsefulOntClasses = new HashMap<>();
        String ssnIRI = iqasConfig.getIRIForPrefix("ssn", true);
        String geoIRI = iqasConfig.getIRIForPrefix("geo", true);
        String qooIRI = iqasConfig.getIRIForPrefix("qoo", true);
        String m3IRI = iqasConfig.getIRIForPrefix("m3-lite", true);
        allUsefulOntClasses.put("ssn:Observation", qooBaseModel.getOntClass( ssnIRI + "Observation" ));
        allUsefulOntClasses.put("ssn:Sensor", qooBaseModel.getOntClass( ssnIRI + "Sensor" ));
        allUsefulOntClasses.put("geo:Point", qooBaseModel.getOntClass( geoIRI + "Point" ));
        allUsefulOntClasses.put("ssn:Property", qooBaseModel.getOntClass( ssnIRI + "Property" ));
        allUsefulOntClasses.put("ssn:SensorOutput", qooBaseModel.getOntClass( ssnIRI + "SensorOutput" ));
        allUsefulOntClasses.put("ssn:ObservationValue", qooBaseModel.getOntClass( ssnIRI + "ObservationValue" ));
        allUsefulOntClasses.put("qoo:QoOIntrisicQuality", qooBaseModel.getOntClass( qooIRI + "QoOIntrisicQuality" ));
        allUsefulOntClasses.put("qoo:QoOValue", qooBaseModel.getOntClass( qooIRI + "QoOValue" ));
        allUsefulOntClasses.put("m3-lite:Millisecond", qooBaseModel.getOntClass( m3IRI + "Millisecond" ));
        allUsefulOntClasses.put("m3-lite:Others", qooBaseModel.getOntClass( m3IRI + "Others" ));
        allUsefulOntClasses.put("m3-lite:Percent", qooBaseModel.getOntClass( m3IRI + "Percent" ));
        return allUsefulOntClasses;
    }

    public static OntModel loadQoOntoWithImports(Config iqasConfig) {
        OntDocumentManager mgr = new OntDocumentManager();
        if (iqasConfig.getOntoConfig().load_from_disk) {
            for (String prefix : iqasConfig.getAllPrefixes()) {
                if (iqasConfig.getFilePathForPrefix(prefix) != null) {
                    String IRI = iqasConfig.getIRIForPrefix(prefix, false);
                    mgr.addAltEntry( IRI, iqasConfig.getFilePathForPrefix(prefix));
                }
            }
            mgr.setProcessImports(true);
        }
        else {
            mgr.setProcessImports(false);
        }

        // Loading of the QoOnto with imports according to iQAS configuration
        OntModelSpec ontModelSpec = new OntModelSpec(OWL_MEM);
        ontModelSpec.setDocumentManager(mgr);
        OntModel baseQoO = ModelFactory.createOntologyModel(ontModelSpec);
        baseQoO.read(iqasConfig.getFilePathForPrefix("qoo"), "RDF/XML");

        return baseQoO;
    }
}
