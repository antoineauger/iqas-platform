package fr.isae.iqas.utils;

import fr.isae.iqas.config.Config;
import org.apache.jena.ontology.OntDocumentManager;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.rdf.model.ModelFactory;

import static org.apache.jena.ontology.OntModelSpec.OWL_MEM;

/**
 * Created by an.auger on 26/04/2017.
 */
public class JenaUtils {
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
