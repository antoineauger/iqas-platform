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
import akka.http.javadsl.server.Route;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import com.mongodb.ConnectionString;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoDatabase;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.model.messages.Terminated;
import fr.isae.iqas.server.APIGatewayActor;
import fr.isae.iqas.server.RESTServer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

/**
 * Created by an.auger on 13/09/2016.
 */
public class MainClass {

    public static class LocalMaster extends UntypedActor {
        LoggingAdapter log = Logging.getLogger(getContext().system(), this);

        public LocalMaster(Properties prop, ActorSystem system, Materializer materializer) throws IOException {
            // Path resolution for the APIGatewayActor
            String pathAPIGatewayActor = getSelf().path().toString() + "/" + prop.getProperty("api_gateway_actor_name");

            // Database initialization
            MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb://"
                    + prop.getProperty("database_endpoint_address") + ":" + prop.getProperty("database_endpoint_port")));

            MongoDatabase database = mongoClient.getDatabase("iqas");
            MongoController mongoController = new MongoController(database, getContext(), pathAPIGatewayActor);

            // MongoDB initialization
            mongoController.dropIQASDatabase();
            mongoController.putSensorsFromFileIntoDB("templates/sensors.json");

            // API Gateway actor creation
            final ActorRef apiGatewayActor = getContext().actorOf(Props.create(APIGatewayActor.class, prop, mongoController),
                    prop.getProperty("api_gateway_actor_name"));

            // REST server creation
            final RESTServer app = new RESTServer(mongoController, apiGatewayActor);
            final Route route = app.createRoute();
            final Flow<HttpRequest, HttpResponse, NotUsed> handler = route.flow(system, materializer);
            final CompletionStage<ServerBinding> binding = Http.get(system).bindAndHandle(handler,
                    ConnectHttp.toHost((String) prop.get("api_gateway_endpoint_address"),
                            Integer.valueOf((String) prop.get("api_gateway_endpoint_port"))), materializer);

            // High-level error handling
            binding.exceptionally(failure -> {
                log.error("Something very bad happened! " + failure.getMessage());
                system.terminate();
                return null;
            });

            // We add a shutdown hook to try gracefully to unbind server when possible
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    log.info("Gracefully shutting down autonomic loops and API gateway...");
                    mongoClient.close();
                    binding.thenCompose(ServerBinding::unbind)
                            .thenAccept(unbound -> system.terminate());
                }
            });
        }

        @Override
        public void onReceive(Object message) throws Throwable {
            if (message instanceof Terminated) {
                log.info("Received Terminated message: {}", message);
                getContext().system().stop(self());
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // Reading iQAS configuration
        Properties prop = new Properties();
        InputStream input = MainClass.class.getClassLoader().getResourceAsStream("iqas.properties");
        prop.load(input);

        // Top-level actors creation
        final ActorSystem system = ActorSystem.create("SystemActor");
        final Materializer materializer = ActorMaterializer.create(system);

        final ActorRef localMaster = system.actorOf(
                Props.create(LocalMaster.class, prop, system, materializer), "LocalMasterActor");
    }


}
