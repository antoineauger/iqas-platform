package fr.isae.iqas.mapek;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.*;
import akka.http.javadsl.server.Route;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import fr.isae.iqas.server.RESTServer;

import java.util.concurrent.CompletionStage;

/**
 * Created by an.auger on 13/09/2016.
 */
public class Test extends UntypedActor {

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("MySystem");
        final Materializer materializer = ActorMaterializer.create(system);

        // REST server creation
        final RESTServer app = new RESTServer();
        final Route route = app.createRoute();
        final Flow<HttpRequest, HttpResponse, NotUsed> handler = route.flow(system, materializer);
        final CompletionStage<ServerBinding> binding = Http.get(system).bindAndHandle(handler, ConnectHttp.toHost("127.0.0.1", 8080), materializer);

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
        Runtime.getRuntime().addShutdownHook(new Thread(() -> binding
                .thenCompose(ServerBinding::unbind)
                .thenAccept(unbound -> system.terminate())));
    }

    @Override
    public void onReceive(Object o) throws Throwable {

    }
}
