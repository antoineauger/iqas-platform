package fr.isae.iqas;

import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.reasoner.ValidityReport;
import org.apache.jena.util.FileManager;
import org.apache.jena.util.PrintUtil;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDFS;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.logging.Level;

import static org.apache.jena.ontology.OntModelSpec.*;

/**
 * Created by an.auger on 17/01/2017.
 */
public class MainClass {
    private static Logger logger = Logger.getLogger(MainClass.class);

    static void printStatements(Model m, Resource s, Property p, Resource o) {
        for (StmtIterator i = m.listStatements(s, p, o); i.hasNext(); ) {
            Statement stmt = i.nextStatement();
            System.out.println(" - " + PrintUtil.print(stmt));
        }
    }

    public static void main(String[] args) throws Exception {

        // Make a TDB-backed dataset
        String schemaLocation = "/Users/an.auger/Desktop/pizza_clean.owl";
        String directory = "/Users/an.auger/Software/TDB_store";
        String ontologyIRI = "http://www.co-ode.org/ontologies/pizza/pizza.owl";
        String NS = ontologyIRI + "#";

        OntModel base = ModelFactory.createOntologyModel( OntModelSpec.OWL_MEM );

        OntModel ontModel = ModelFactory.createOntologyModel( OntModelSpec.OWL_MEM_RULE_INF );
        ontModel.read( schemaLocation );
        OntClass a = ontModel.getOntClass( NS + "Giardiniera" );
        OntClass c = ontModel.getOntClass(NS + "CheeseyPizza");

        Reasoner reasoner = ReasonerRegistry.getOWLReasoner();
        reasoner = reasoner.bindSchema(ontModel);
        reasoner.setDerivationLogging(true);

        InfModel infModel = ModelFactory.createInfModel (reasoner, ontModel);

        StmtIterator i = infModel.listStatements(a, null, (RDFNode) null);
        while(i.hasNext()){
            Statement s = i.nextStatement();
            System.out.println("Statement: \n" + s.asTriple()); //turtle format
        }

        FileManager.get().addLocatorClassLoader(MainClass.class.getClassLoader());

        //Model tbox = FileManager.get().loadModel("/Users/an.auger/Desktop/pizza_clean.owl", null, "RDF/XML"); // http://en.wikipedia.org/wiki/Tbox
        OntModel base2 = ModelFactory.createOntologyModel(OWL_MEM);
        FileInputStream in = null;
        try {
            in = new FileInputStream("/Users/an.auger/Desktop/pizza_clean.owl");
        } catch(FileNotFoundException ex)
        {
            logger.error(ex);
        }
        base2.read(in,null,null);

        OntModel inf = ModelFactory.createOntologyModel(OWL_MEM_MICRO_RULE_INF, base2);
        OntClass paper = base2.getOntClass(NS + "FourSeasons");
        Individual p1 = base2.createIndividual(NS + "paper1", paper);

        /*Reasoner reasoner = ReasonerRegistry.getOWLMicroReasoner();
        reasoner.bindSchema(ontModel);

        InfModel infModel = ModelFactory.createInfModel(reasoner, ontModel);
        Resource a = infModel.getResource( NS + "FourSeasons" );
        Resource b = infModel.getResource( NS + "CheeseyPizza" );

        if (infModel.contains(a, RDFS.subClassOf, b)) {
            System.out.println("OK");
        }
        else {
            System.out.println("NOK");
        }*/

        // list the inferred types
        p1 =inf.getIndividual(NS +"paper1");
        for(Iterator<Resource> j = p1.listRDFTypes(true); i.hasNext(); )
        {
            System.out.println(p1.getURI() + " is inferred to be in class " + j.next());
        }

        OntClass artefact = inf.getOntClass(NS + "FourSeasons");
        for(Iterator<OntClass> k = artefact.listSubClasses(); i.hasNext(); )
        {
            OntClass d = k.next();
            System.out.println(d.getURI());
        }

        /*StmtIterator i = infModel.listStatements(a, null, (RDFNode) null);
        while(i.hasNext()){
            Statement s = i.nextStatement();
            System.out.println("Statement: \n" + s.asTriple()); //turtle format
        }

        StmtIterator j = infModel.listStatements(b, null, (RDFNode) null);
        while(j.hasNext()){
            Statement s = j.nextStatement();
            System.out.println("Statement: \n" + s.asTriple()); //turtle format
        }*/
}

}
