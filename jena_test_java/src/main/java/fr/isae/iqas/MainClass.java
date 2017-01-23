package fr.isae.iqas;

import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.util.PrintUtil;

import java.util.Iterator;

import static org.apache.jena.ontology.OntModelSpec.OWL_MEM_MICRO_RULE_INF;

/**
 * Created by an.auger on 17/01/2017.
 */
public class MainClass {
    static void printStatements(Model m, Resource s, Property p, Resource o) {
        for (StmtIterator i = m.listStatements(s, p, o); i.hasNext(); ) {
            Statement stmt = i.nextStatement();
            System.out.println(" - " + PrintUtil.print(stmt));
        }
    }

    public static void main(String[] args) throws Exception {
        String schemaLocation = "file:/Users/an.auger/Desktop/iQAS_ontology/qoo-ontology.rdf";
        String qooIRI = "http://isae.fr/iqas/qoo-ontology#";
        String ssnIRI = "http://purl.oclc.org/NET/ssnx/ssn#";
        String iotliteIRI = "http://purl.oclc.org/NET/UNIS/fiware/iot-lite#";
        String geoIRI = "http://www.w3.org/2003/01/geo/wgs84_pos#";

        OntDocumentManager mgr = new OntDocumentManager();
        //mgr.setProcessImports(false);
        OntModelSpec s = new OntModelSpec( OntModelSpec.OWL_MEM);
        s.setDocumentManager(mgr);
        OntModel ontModelBase = ModelFactory.createOntologyModel(s);
        ontModelBase.read(schemaLocation, "RDF/XML");

        Model otherModel = ontModelBase.read("file:/Users/an.auger/Desktop/iQAS_ontology/sensors.jsonld", "JSON-LD");

        ontModelBase.add(otherModel);

        // create the reasoning model using the base
        OntModel inf = ModelFactory.createOntologyModel( OWL_MEM_MICRO_RULE_INF, ontModelBase );

        //Properties
        Property onPlatform = inf.getProperty( ssnIRI + "onPlatform" );
        Property provides = inf.getProperty( qooIRI + "provides" );
        Property supports = inf.getProperty( qooIRI + "supports" );
        Property isSupportedBy = inf.getProperty( qooIRI + "isSupportedBy" );


        Individual iqas = inf.getIndividual( qooIRI + "iQAS" );
        Property attachedSystem = inf.getProperty(ssnIRI + "attachedSystem");
        NodeIterator attachedToIQAS = iqas.listPropertyValues(attachedSystem);
        for (NodeIterator i = attachedToIQAS; i.hasNext(); ) {
            System.out.println( "Attached: " + i.next() );
        }


        //TODO replace
        OntClass sensingDevice = inf.getOntClass( qooIRI + "visibility" );
        Individual sensor01 = inf.getIndividual( qooIRI + "visibility" );


        /*List<RDFNode> r = sensor01.listPropertyValues(hasMeasurementCapability).toList();
        Individual o = inf.getIndividual(r.get(0).toString());

        RDFNode q = o.getPropertyValue(hasMeasurementProperty);
        Individual p = inf.getIndividual(q.toString());*/


        // list the asserted types
        for (Iterator<Resource> i = sensor01.listRDFTypes(false); i.hasNext(); ) {
            System.out.println( iqas.getURI() + " is asserted in class " + i.next() );
        }

        // list the inferred types
        for (Iterator<Resource> i = sensor01.listRDFTypes(false); i.hasNext(); ) {
            System.out.println( iqas.getURI() + " is inferred to be in class " + i.next() );
        }

        // list the individuals
        /*for (ExtendedIterator<? extends OntResource> i = sensor01.listInstances(); i.hasNext(); ) {
            System.out.println( i.next() );
        }*/

        // list the properties
        for (StmtIterator i = sensor01.listProperties(); i.hasNext(); ) {
            System.out.println( i.next() );
        }




        //Tests

        /*if (inf.contains(node1, onPlatform, iqas)) {
            System.out.println("Node1 is on platform iQAS");
        } else {
            System.out.println("Node1 IS NOT on platform iQAS");
        }

        if (inf.contains(iqas, provides, filteringMech)) {
            System.out.println("iQAS provides Filtering mechanism");
        } else {
            System.out.println("iQAS DOES NOT provide Filtering mechanism");
        }

        for (ExtendedIterator<Individual> i = inf.listIndividuals(operators); i.hasNext(); ) {
            Individual curr = i.next();
            if (inf.contains(curr, isSupportedBy, filteringMech)) {
                System.out.println("FilteringMech supports " + curr.getLocalName() + " operator");
            } else {
                System.out.println("FilteringMech DOES NOT support " + curr.getLocalName() + " operator");
            }
        }*/
}

}
