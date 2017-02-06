package fr.isae.iqas;

import com.fasterxml.jackson.databind.util.JSONPObject;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import org.apache.jena.ontology.*;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.jena.util.PrintUtil;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

    public static void execSelectAndProcess(String serviceURI, String query) throws JsonLdError, IOException {
        QueryExecution q = QueryExecutionFactory.sparqlService(serviceURI,
                query);

        ResultSet m = q.execSelect();
        //printStatements(m, null, null, null);

        //For DESCRIBE
//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        m.write(outputStream, "JSON-LD");
//        Map context = new HashMap();
//        JsonLdOptions options = new JsonLdOptions();
//        Object compact = JsonLdProcessor.expand(JsonUtils.fromString(new String(outputStream.toByteArray())), options);
//        System.out.println(JsonUtils.toPrettyString(compact));
//
//        m.write(System.out, "JSON-LD");

        //Model m = q.execConstruct();
        /*QueryEngineHTTP qeHttp = (QueryEngineHTTP) q;
        qeHttp.setModelContentType("application/rdf+xml");
// Serialize it
        Model m = q.execConstruct();
        m.write(System.out, "TTL");
        m.write(System.out, "JSON-LD");*/

        //ResultSet results = null;
        try {
            //ResultSet results = q.execSelect();
            //results = q.execSelect();

            ResultSetFormatter.outputAsJSON(System.out, m);
        }
        finally {
            q.close();
        }




        // write to a ByteArrayOutputStream
        /*ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        ResultSetFormatter.outputAsJSON(outputStream, results);

        // and turn that into a String
        String json = new String(outputStream.toByteArray());

        JSONObject t = new JSONObject("{ \"type\": \"uri\" , \"value\": \"http://isae.fr/iqas/qoo-ontology#iQAS\" }");
        JSONObject s = new JSONObject(t.get("results");


// Read the file into an Object (The type of this object will be a List, Map, String, Boolean,
// Number or null depending on the root object in the file).
        Object jsonObject = JsonUtils.fromString(json);
// Create a context JSON map containing prefixes and definitions
        Map context = new HashMap();
        context.put("qoo", "http://isae.fr/iqas/qoo-ontology#");
// Customise context...
// Create an instance of JsonLdOptions with the standard JSON-LD options
        JsonLdOptions options = new JsonLdOptions();
// Customise options...
// Call whichever JSONLD function you want! (e.g. compact)
        Object compact = JsonLdProcessor.compact(jsonObject, context, options);
// Print out the result (or don't, it's your call!)
        System.out.println(JsonUtils.toPrettyString(compact));

        System.out.println(json);

        /*while (results.hasNext()) {
            QuerySolution soln = results.nextSolution();
            // assumes that you have an "?x" in your query
            RDFNode x = soln.get("subject");
            System.out.println(x);
        }*/
    }

    public static void main(String[] args) throws Exception {
        String schemaLocation = "file:/Users/an.auger/Documents/GIT/iQAS_platform/jena_test_java/iQAS_ontology/qoo-ontology.rdf";
        String qooIRI = "http://isae.fr/iqas/qoo-ontology#";
        String ssnIRI = "http://purl.oclc.org/NET/ssnx/ssn#";
        String iotliteIRI = "http://purl.oclc.org/NET/UNIS/fiware/iot-lite#";
        String geoIRI = "http://www.w3.org/2003/01/geo/wgs84_pos#";

        execSelectAndProcess(
                "http://localhost:3030/qoo-onto/sparql",
                "PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#>\n" +
                        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                        "PREFIX qoo: <http://isae.fr/iqas/qoo-ontology#>\n" +
                        "PREFIX iot-lite: <http://purl.oclc.org/NET/UNIS/fiware/iot-lite#>\n" +
                        "PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>\n" +
                        "SELECT ?sensor ?topic ?longitude ?latitude ?location\n" +
                        "WHERE {\n" +
                        "?sensor iot-lite:id \"sensor02\" .\n" +
                        "?sensor ssn:madeObservation ?o .\n" +
                        "?o ssn:observedProperty ?topic .\n" +
                        "?sensor geo:location ?pos .\n" +
                        "?pos geo:long ?longitude .\n" +
                        "?pos geo:lat ?latitude .\n" +
                        "?pos iot-lite:relativeLocation ?location\n" +
                        "}");


//        OntDocumentManager mgr = new OntDocumentManager();
//        //mgr.setProcessImports(false);
//        OntModelSpec s = new OntModelSpec( OntModelSpec.OWL_MEM);
//        s.setDocumentManager(mgr);
//        OntModel ontModelBase = ModelFactory.createOntologyModel(s);
//        ontModelBase.read(schemaLocation, "RDF/XML");
//
//        Model otherModel = ontModelBase.read("file:/Users/an.auger/Documents/GIT/iQAS_platform/jena_test_java/iQAS_ontology/sensors.jsonld", "JSON-LD");
//
//        ontModelBase.union(otherModel);
//
//        // create the reasoning model using the base
//        OntModel inf = ModelFactory.createOntologyModel( OWL_MEM_MICRO_RULE_INF, ontModelBase );
//
//        //Properties
//        Property onPlatform = inf.getProperty( ssnIRI + "onPlatform" );
//        Property provides = inf.getProperty( qooIRI + "provides" );
//        Property supports = inf.getProperty( qooIRI + "supports" );
//        Property isSupportedBy = inf.getProperty( qooIRI + "isSupportedBy" );
//
//
//        Individual iqas = inf.getIndividual( qooIRI + "iQAS" );
//        Property attachedSystem = inf.getProperty(ssnIRI + "attachedSystem");
//        NodeIterator attachedToIQAS = iqas.listPropertyValues(attachedSystem);
//        for (NodeIterator i = attachedToIQAS; i.hasNext(); ) {
//            System.out.println( "Attached: " + i.next() );
//        }
//
//
//        //TODO replace
//        OntClass sensingDevice = inf.getOntClass( qooIRI + "visibility" );
//        Individual sensor01 = inf.getIndividual( qooIRI + "sensor01" );
//
//
//        /*List<RDFNode> r = sensor01.listPropertyValues(hasMeasurementCapability).toList();
//        Individual o = inf.getIndividual(r.get(0).toString());
//
//        RDFNode q = o.getPropertyValue(hasMeasurementProperty);
//        Individual p = inf.getIndividual(q.toString());*/
//
//
//        // list the asserted types
//        for (Iterator<Resource> i = sensor01.listRDFTypes(false); i.hasNext(); ) {
//            System.out.println( iqas.getURI() + " is asserted in class " + i.next() );
//        }
//
//        // list the inferred types
//        for (Iterator<Resource> i = sensor01.listRDFTypes(false); i.hasNext(); ) {
//            System.out.println( iqas.getURI() + " is inferred to be in class " + i.next() );
//        }
//
//        // list the individuals
//        /*for (ExtendedIterator<? extends OntResource> i = sensor01.listInstances(); i.hasNext(); ) {
//            System.out.println( i.next() );
//        }*/
//
//        // list the properties
//        for (StmtIterator i = sensor01.listProperties(); i.hasNext(); ) {
//            System.out.println( i.next() );
//        }




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
