package fr.isae.iqas;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
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
import com.mongodb.reactivestreams.client.*;
import fr.isae.iqas.database.SubscriberHelpers.ObservableSubscriber;
import fr.isae.iqas.server.RESTServer;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

/**
 * Created by an.auger on 13/09/2016.
 */
public class MainClass extends UntypedActor {

    public static void main(String[] args) throws IOException, InterruptedException {
        // Reading iQAS configuration
        Properties prop = new Properties();
        InputStream input = MainClass.class.getClassLoader().getResourceAsStream("iqas.properties");
        prop.load(input);

        // Database initialization
        MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb://"
                + prop.get("database_endpoint_address") + ":" + prop.get("database_endpoint_port")));
        MongoDatabase database = mongoClient.getDatabase("iqas");
        MongoCollection<Document> collection = database.getCollection("sensors");
        ObservableSubscriber subscriber = new ObservableSubscriber<Success>();

        // Sensors declaration
        List<Document> documents = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(MainClass.class.getClassLoader().getResourceAsStream("sensors.json"), StandardCharsets.UTF_8))) {
            String sCurrentLine;
            while ((sCurrentLine = reader.readLine()) != null) {
                documents.add(Document.parse(sCurrentLine));
            }
            collection.insertMany(documents).subscribe(subscriber);
            subscriber.await();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        // Top-level actors creation
        final ActorSystem system = ActorSystem.create("MySystem");
        final Materializer materializer = ActorMaterializer.create(system);

        // REST server creation
        final RESTServer app = new RESTServer();
        final Route route = app.createRoute();
        final Flow<HttpRequest, HttpResponse, NotUsed> handler = route.flow(system, materializer);
        final CompletionStage<ServerBinding> binding = Http.get(system).bindAndHandle(handler,
                ConnectHttp.toHost((String) prop.get("api_gateway_endpoint_address"),
                        Integer.valueOf((String) prop.get("api_gateway_endpoint_port"))), materializer);

        // High-level error handling
        binding.exceptionally(failure -> {
            System.err.println("Something very bad happened! " + failure.getMessage());
            system.terminate();
            return null;
        });

        // Actors for Information Layer
        //final ActorRef infoMonitorActor = system.actorOf(Props.create(MonitorActor.class), "infoMonitorActor");
        //final ActorRef infoAnalyzeActor = system.actorOf(Props.create(AnalyzeActor.class, infoMonitorActor), "infoAnalyzeActor");

        /*infoMonitorActor.tell(new AddKafkaTopic("topic1"), infoAnalyzeActor);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        infoMonitorActor.tell(new AddKafkaTopic("topic2"), infoAnalyzeActor);


        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        infoMonitorActor.tell(new AddKafkaTopic("terminate"), infoAnalyzeActor);*/

        // We add a shutdown hook to try gracefully to unbind server when possible
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Gracefully shutting down autonomic loops and API gateway...");
                mongoClient.close();
                binding.thenCompose(ServerBinding::unbind)
                        .thenAccept(unbound -> system.terminate());
            }
        });

    }

    @Override
    public void onReceive(Object o) throws Throwable {

    }


}
