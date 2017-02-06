package fr.isae.iqas.database;

import akka.actor.ActorRef;
import akka.actor.UntypedActorContext;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import akka.util.Timeout;
import com.fasterxml.jackson.databind.ObjectMapper;
import ioinformarics.oss.jackson.module.jsonld.JsonldModule;
import ioinformarics.oss.jackson.module.jsonld.JsonldResource;
import ioinformarics.oss.jackson.module.jsonld.JsonldResourceBuilder;
import org.apache.log4j.Logger;
import scala.concurrent.Future;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 03/02/2017.
 */
public class FusekiRESTController extends AllDirectives {
    private static Logger log = Logger.getLogger(FusekiRESTController.class);

    private String baseQoOIRI = null;
    private FusekiController controller = null;
    private UntypedActorContext context;
    private String pathAPIGatewayActor;

    public FusekiRESTController(Properties prop, UntypedActorContext context, String pathAPIGatewayActor) {
        this.controller = new FusekiController(prop);

        String fullQoOIRI = prop.getProperty("qooIRI");
        this.baseQoOIRI = fullQoOIRI.substring(0,fullQoOIRI.length()-1);
        this.context = context;
        this.pathAPIGatewayActor = pathAPIGatewayActor;
    }

    public FusekiController getController() {
        return controller;
    }

    public Future<ActorRef> getAPIGatewayActor() {
        return context.actorSelection(pathAPIGatewayActor).resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

    /**
     * Sensors
     * @param ctx
     */

    public CompletableFuture<Route> getAllSensors(Executor ctx) {
        CompletableFuture<Route> sensors;
        ObjectMapper mapper = new ObjectMapper();

        sensors = CompletableFuture.supplyAsync(() -> controller._findAllSensors(), ctx).thenApply((result) -> {
            if (result == null) {
                return complete(HttpResponse.create()
                        .withStatus(400)
                        .withEntity("No sensor has been registered yet!"));
            }
            else {
                mapper.registerModule(new JsonldModule(mapper::createObjectNode));
                JsonldResourceBuilder builder = JsonldResource.Builder.create();
                builder.context(baseQoOIRI);
                return completeOK(builder.build(result), Jackson.marshaller(mapper));
            }
        });

        return sensors;
    }

    public CompletableFuture<Route> getSensor(String sensor_id, Executor ctx) {
        CompletableFuture<Route> sensor;
        ObjectMapper mapper = new ObjectMapper();

        sensor = CompletableFuture.supplyAsync(() -> controller._findSpecificSensor(sensor_id), ctx).thenApply((result) -> {
            if (result == null) {
                return complete(HttpResponse.create()
                        .withStatus(400)
                        .withEntity("No sensor found with @id \"" + baseQoOIRI + "#" + sensor_id + "\""));
            }
            else {
                mapper.registerModule(new JsonldModule(mapper::createObjectNode));
                JsonldResourceBuilder builder = JsonldResource.Builder.create();
                builder.context(baseQoOIRI);
                return completeOK(builder.build(result), Jackson.marshaller(mapper));
            }
        });

        return sensor;
    }

}
