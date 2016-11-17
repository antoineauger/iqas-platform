package fr.isae.iqas.server;

import akka.actor.ActorRef;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.database.MongoRESTController;
import fr.isae.iqas.model.request.Request;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static akka.http.javadsl.server.PathMatchers.segment;

/**
 * Created by an.auger on 16/09/2016.
 */

public class RESTServer extends AllDirectives {
    private MongoRESTController mongoRESTController;
    private ActorRef apiGatewayActor;

    public RESTServer(MongoRESTController mongoRESTController, ActorRef apiGatewayActor) {
        this.mongoRESTController = mongoRESTController;
        this.apiGatewayActor = apiGatewayActor;
    }

    private CompletionStage<Route> getSensor(Executor ctx, String sensor_id) {
        return CompletableFuture.supplyAsync(() -> mongoRESTController.getSensor(sensor_id), ctx);
    }

    private CompletionStage<Route> getAllSensors(Executor ctx) {
        return CompletableFuture.supplyAsync(() -> mongoRESTController.getAllSensors(), ctx);
    }

    // TODO : put sensor method here ?

    private CompletionStage<Route> getRequest(Executor ctx, String request_id) {
        return CompletableFuture.supplyAsync(() -> mongoRESTController.getRequest(request_id), ctx);
    }

    private CompletionStage<Route> getAllRequests(Executor ctx) {
        return CompletableFuture.supplyAsync(() -> mongoRESTController.getAllRequests(), ctx);
    }

    private CompletionStage<Route> putRequest(Executor ctx, Request req) {
        return CompletableFuture.supplyAsync(() -> mongoRESTController.putRequest(req), ctx);
    }

    public Route createRoute() {

        Route iqasRequest =
                parameterOptional("name", new Function<Optional<String>, Route>() {
                    @Override
                    public Route apply(Optional<String> optName) {
                        String name = optName.orElse("Mister X");

                        // Build request

                        // Possible? YES / NO

                        // Get ticket number

                        // Forward request to API gateway

                        //return extractExecutionContext(ctx -> onSuccess(() -> mongoController.getSensor(), Function.identity()));
                        //CompletionStage<List<VirtualSensor>> result = mongoController.getSensor();

                        //return completeOKWithFuture(result, Jackson.<List<VirtualSensor>>marshaller());
                        return null;
                    }
                });

        return
                route(
                        get(() -> route(
                                // matches the empty path
                                pathSingleSlash(() ->
                                        // homepage + TODO : API overview
                                        getFromResource("web/index.html", ContentTypes.TEXT_HTML_UTF8)
                                ),
                                path(segment("sensors"), () ->
                                        extractExecutionContext(ctx ->
                                                onSuccess(() -> getAllSensors(ctx), Function.identity())
                                        )
                                ),
                                path(segment("sensors").slash(segment()), (sensor_id) ->
                                        extractExecutionContext(ctx ->
                                                onSuccess(() -> getSensor(ctx, sensor_id), Function.identity())
                                        )
                                ),
                                path(segment("requests"), () ->
                                        extractExecutionContext(ctx ->
                                                onSuccess(() -> getAllRequests(ctx), Function.identity())
                                        )
                                ),
                                path(segment("requests").slash(segment()), (request_id) ->
                                        extractExecutionContext(ctx ->
                                                onSuccess(() -> getRequest(ctx, request_id), Function.identity())
                                        )
                                )
                        )),
                        put(() -> route(
                                path(segment("requests"), () ->
                                        entity(Jackson.unmarshaller(Request.class), myRequest ->
                                                extractExecutionContext(ctx ->
                                                    onSuccess(() -> putRequest(ctx, myRequest), Function.identity())
                                                )
                                        ).orElse(
                                                complete(HttpResponse.create()
                                                        .withStatus(400)
                                                        .withEntity("Malformed request submitted!")
                                                )
                                        )
                                )
                        )),
                        get(() -> complete(
                                HttpResponse.create()
                                        .withStatus(404)
                                        .withEntity("Unknown API endpoint!"))
                        ),
                        post(() -> complete(
                                HttpResponse.create()
                                        .withStatus(404)
                                        .withEntity("Unknown API endpoint!"))
                        ),
                        put(() -> complete(
                                HttpResponse.create()
                                        .withStatus(404)
                                        .withEntity("Unknown API endpoint!"))
                        )
                );
    }

}
