package fr.isae.iqas;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.mongodb.ConnectionString;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoDatabase;
import fr.isae.iqas.config.Config;
import fr.isae.iqas.config.OntoConfig;
import fr.isae.iqas.database.FusekiRESTController;
import fr.isae.iqas.database.MongoRESTController;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.pipelines.PipelineWatcherActor;
import fr.isae.iqas.server.APIGatewayActor;
import fr.isae.iqas.server.RESTServer;
import org.apache.commons.io.IOUtils;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

import static org.apache.jena.ontology.OntModelSpec.OWL_MEM;

/**
 * Created by an.auger on 13/09/2016.
 */
public class MainClass extends AllDirectives{

    private static class LocalMaster extends UntypedActor {
        private LoggingAdapter log;

        public LocalMaster(Config iqasConfig, ActorSystem system, Http http, Materializer materializer) {
            log = Logging.getLogger(getContext().system(), this);
            Properties prop = iqasConfig.getProp();

            // Path resolution for the APIGatewayActor
            String pathAPIGatewayActor = getSelf().path().toString() + "/" + prop.getProperty("api_gateway_actor_name");
            String pathPipelineWatcherActor = getSelf().path().toString() + "/" + "pipelineWatcherActor";

            // Database initialization
            MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb://"
                    + prop.getProperty("database_endpoint_address") + ":" + prop.getProperty("database_endpoint_port")));
            MongoDatabase database = mongoClient.getDatabase("iqas");

            // MongoController to perform management queries (requests, mapek logging)
            MongoRESTController mongoRESTController = new MongoRESTController(database);
            mongoRESTController.getController().dropIQASDatabase();

            // FusekiController to perform SPARQL queries (sensors, pipelines)
            FusekiRESTController fusekiRESTController = new FusekiRESTController(iqasConfig, getContext(), pathPipelineWatcherActor);

            // Watcher for dynamic QoO pipelines addition / removal
            final ActorRef pipelineWatcherActor = getContext().actorOf(Props.create(PipelineWatcherActor.class, iqasConfig), "pipelineWatcherActor");

            // API Gateway actor creation
            final ActorRef apiGatewayActor = getContext().actorOf(Props.create(APIGatewayActor.class, iqasConfig,
                    mongoRESTController.getController(), fusekiRESTController.getController()),
                    prop.getProperty("api_gateway_actor_name"));

            // REST server creation
            final RESTServer app = new RESTServer(mongoRESTController, fusekiRESTController, apiGatewayActor);
            final Flow<HttpRequest, HttpResponse, NotUsed> handler = app.createRoute().flow(system, materializer);
            final CompletionStage<ServerBinding> binding = http.bindAndHandle(handler,
                    ConnectHttp.toHost((String) prop.get("api_gateway_endpoint_address"),
                            Integer.valueOf((String) prop.get("api_gateway_endpoint_port"))), materializer);

            // High-level error handling
            binding.exceptionally(failure -> {
                log.error("Something very bad happened! " + failure.getMessage());
                system.terminate();
                return null;
            });

            // We add a shutdown hook to try gracefully to unbind server when possible
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Gracefully shutting down autonomic loops and API gateway...");
                mongoClient.close();
                binding.thenCompose(ServerBinding::unbind)
                        .thenAccept(unbound -> system.terminate());
            }));

        }

        @Override
        public void onReceive(Object message) throws Throwable {
            if (message instanceof TerminatedMsg) {
                TerminatedMsg terminatedMsg = (TerminatedMsg) message;
                if (terminatedMsg.getTargetToStop().path().equals(getSelf().path())) {
                    log.info("Received TerminatedMsg message: " + message);
                    getContext().stop(self());
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(MainClass.class);

        // Reading iQAS configuration
        String iqasConfFileName = "iqas.properties";
        String ontologiesConfFileName = "ontologies.yml";

        Properties prop = new Properties();
        try {
            InputStream input1 = MainClass.class.getClassLoader().getResourceAsStream(iqasConfFileName);
            prop.load(input1);
        } catch (NullPointerException e) {
            logger.error("Unable to find the configuration file '" + iqasConfFileName + "'.");
            System.exit(1);
        }

        OntoConfig ontoConfig = null;
        try {
            InputStream input2 = MainClass.class.getClassLoader().getResourceAsStream(ontologiesConfFileName);
            String text = IOUtils.toString(input2, StandardCharsets.UTF_8.name());
            YamlReader reader = new YamlReader(text);
            ontoConfig = reader.read(OntoConfig.class);
        } catch (Exception e) {
            logger.error(e.toString());
            System.exit(1);
        }

        Config iqasConfig = new Config(prop, ontoConfig);

        // Top-level actors creation
        final ActorSystem system = ActorSystem.create("SystemActor");
        final Http http = Http.get(system);
        final Materializer materializer = ActorMaterializer.create(system);
        final ActorRef localMaster = system.actorOf(Props.create(LocalMaster.class, iqasConfig, system, http, materializer), "LocalMasterActor");

        /*String qooIRI = iqasConfig.getIRIForPrefix("qoo", false);
        String ssnIRI = iqasConfig.getIRIForPrefix("ssn", false);
        String iotliteIRI = iqasConfig.getIRIForPrefix("iot-lite", false);
        String dulIRI = iqasConfig.getIRIForPrefix("DUL", false);
        String geoIRI = iqasConfig.getIRIForPrefix("geo", false);
        String quIRI = iqasConfig.getIRIForPrefix("qu", false);
        String m3IRI = iqasConfig.getIRIForPrefix("m3-lite", false);

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
        OntModelSpec s = new OntModelSpec( OntModelSpec.OWL_MEM);
        s.setDocumentManager(mgr);
        OntModel baseQoO = ModelFactory.createOntologyModel(s);
        baseQoO.read(iqasConfig.getFilePathForPrefix("qoo"), "RDF/XML");

        // We describe the observation in this model
        OntModel obsModel = ModelFactory.createOntologyModel(OWL_MEM);


        OntClass observation = baseQoO.getOntClass( ssnIRI + "Observation" );
        Individual obs = obsModel.createIndividual( qooIRI + "obs", observation );

        OntClass virtualSensor = baseQoO.getOntClass( ssnIRI + "Sensor" );
        Individual sensor = obsModel.createIndividual( qooIRI + "sensor01", virtualSensor );

        Property observedByProp = baseQoO.getProperty( ssnIRI + "observedBy" );
        obs.setPropertyValue(observedByProp, sensor);

        OntClass point = baseQoO.getOntClass( geoIRI + "Point" );
        Individual locSensor = obsModel.createIndividual( qooIRI + "loc_toulouse", point );

        Property latProp = baseQoO.getProperty( geoIRI + "lat" );
        Property longProp = baseQoO.getProperty( geoIRI + "long" );
        Property altProp = baseQoO.getProperty( geoIRI + "alt" );
        Property relativeLocationProp = baseQoO.getProperty( iotliteIRI + "relativeLocation" );
        Property altRelativeProp = baseQoO.getProperty( iotliteIRI + "altRelative" );

        locSensor.setPropertyValue(latProp, baseQoO.createLiteral("43.601176"));
        locSensor.setPropertyValue(longProp, baseQoO.createLiteral("1.439363"));
        locSensor.setPropertyValue(altProp, baseQoO.createLiteral("0"));
        locSensor.setPropertyValue(relativeLocationProp, baseQoO.createLiteral("Toulouse"));
        locSensor.setPropertyValue(altRelativeProp, baseQoO.createLiteral("0"));

        Property locationProp = baseQoO.getProperty( geoIRI + "location" );
        sensor.setPropertyValue(locationProp, locSensor);


        OntClass property = baseQoO.getOntClass( ssnIRI + "Property" );
        Individual propSensor = obsModel.createIndividual( qooIRI + "temperature", property );
        Property observedPropertyProp = baseQoO.getProperty( ssnIRI + "observedProperty" );
        obs.setPropertyValue(observedPropertyProp, propSensor);


        OntClass sensorOutput = baseQoO.getOntClass( ssnIRI + "SensorOutput" );
        Individual sensorOutputInd = obsModel.createIndividual( qooIRI + "obsOutput", sensorOutput );
        Property hasValueProp = baseQoO.getProperty( ssnIRI + "hasValue" );

        OntClass obsValue = baseQoO.getOntClass( ssnIRI + "ObservationValue" );
        Individual obsValueInd = obsModel.createIndividual( qooIRI + "obsValue", obsValue );
        Property obsValueProp = baseQoO.getProperty( qooIRI + "obsStrValue" );
        Property obsLevelValueProp = baseQoO.getProperty( qooIRI + "levelValue" );

        sensorOutputInd.setPropertyValue(hasValueProp, obsValueInd);
        obsValueInd.setPropertyValue(obsValueProp, baseQoO.createLiteral("-50.6"));
        obsValueInd.setPropertyValue(obsLevelValueProp, baseQoO.createDatatypeProperty("KNOWLEDGE"));

        // Units and Quantity


        Property hasUnitProp = baseQoO.getProperty( iotliteIRI + "hasUnit" );
        Property hasQuantityKindProp = baseQoO.getProperty( iotliteIRI + "hasQuantityKind" );


        OntClass temperatureQK = baseQoO.getOntClass( m3IRI + "Temperature" );
        Individual temperatureInd = obsModel.createIndividual( qooIRI + "temperatureKind", temperatureQK );

        OntClass degreeCelsiusU = baseQoO.getOntClass( m3IRI + "DegreeCelsius" );
        Individual degreeCelsiusInd = obsModel.createIndividual( qooIRI + "temperatureUnit", degreeCelsiusU );

        obsValueInd.setPropertyValue(hasUnitProp, degreeCelsiusInd);
        obsValueInd.setPropertyValue(hasQuantityKindProp, temperatureInd);


        // QoO

        OntClass qoOIntrisicQuality = baseQoO.getOntClass( qooIRI + "QoOIntrisicQuality" );
        Property hasQoOValueProp = baseQoO.getProperty( iotliteIRI + "hasQoOValue" );

        Individual obsQoOFreshnessInd = obsModel.createIndividual( qooIRI + "qooFreshness", qoOIntrisicQuality );
        Individual obsQoOAccuracyInd = obsModel.createIndividual( qooIRI + "qooAccuracy", qoOIntrisicQuality );


        OntClass qoOValue = baseQoO.getOntClass( qooIRI + "QoOValue" );
        Individual obsQoOValueFreshnessInd = obsModel.createIndividual( qooIRI + "qooFreshnessValue", qoOValue );
        Individual obsQoOValueAccuracyInd = obsModel.createIndividual( qooIRI + "qooAccuracyValue", qoOValue );

        obsQoOFreshnessInd.setPropertyValue(hasQoOValueProp, obsQoOValueFreshnessInd);
        obsQoOAccuracyInd.setPropertyValue(hasQoOValueProp, obsQoOValueAccuracyInd);

        OntClass othersQK = baseQoO.getOntClass( m3IRI + "Others" );
        Individual freshnessKindInd = obsModel.createIndividual( qooIRI + "freshnessKind", othersQK );
        Individual accuracyKindInd = obsModel.createIndividual( qooIRI + "accuracyKind", othersQK );

        // OBS_ACCURACY
        Property qooStrValueProp = baseQoO.getProperty( qooIRI + "qooStrValue" );

        OntClass percentageU = baseQoO.getOntClass( m3IRI + "Percent" );
        Individual percentageUInd = obsModel.createIndividual( qooIRI + "accuracyUnit", percentageU );

        obsQoOValueAccuracyInd.setPropertyValue(hasUnitProp, percentageUInd);
        obsQoOValueAccuracyInd.setPropertyValue(hasQuantityKindProp, accuracyKindInd);
        obsQoOValueAccuracyInd.setPropertyValue(qooStrValueProp, baseQoO.createLiteral("100.0"));

        // OBS_FRESHNESS
        OntClass millisecondU = baseQoO.getOntClass( m3IRI + "Millisecond" );
        Individual millisecondUInd = obsModel.createIndividual( qooIRI + "freshnessUnit", millisecondU );

        obsQoOValueFreshnessInd.setPropertyValue(hasUnitProp, millisecondUInd);
        obsQoOValueFreshnessInd.setPropertyValue(hasQuantityKindProp, freshnessKindInd);
        obsQoOValueFreshnessInd.setPropertyValue(qooStrValueProp, baseQoO.createLiteral("542"));


        RDFDataMgr.write(System.out, obsModel, RDFFormat.JSONLD_COMPACT_PRETTY);*/

    }

}
