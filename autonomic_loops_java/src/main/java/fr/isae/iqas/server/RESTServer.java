package fr.isae.iqas.server;

import akka.actor.ActorRef;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import fr.isae.iqas.database.MongoController;

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
    MongoController mongoController;
    ActorRef apiGatewayActor;

    public RESTServer(MongoController mongoController, ActorRef apiGatewayActor) {
        this.apiGatewayActor = apiGatewayActor;
        this.mongoController = mongoController;
    }

    public CompletionStage<Route> getSensor(Executor ctx, String sensor_id) {
        return CompletableFuture.supplyAsync(() -> mongoController.getSensor(sensor_id), ctx);
    }

    public CompletionStage<Route> getAllSensors(Executor ctx) {
        return CompletableFuture.supplyAsync(() -> mongoController.getAllSensors(), ctx);
    }

    // TODO : put sensor method here ?

    public CompletionStage<Route> getRequest(Executor ctx, String request_id) {
        return CompletableFuture.supplyAsync(() -> mongoController.getRequest(request_id), ctx);
    }

    public CompletionStage<Route> getAllRequests(Executor ctx) {
        return CompletableFuture.supplyAsync(() -> mongoController.getAllRequests(), ctx);
    }

    public CompletionStage<Route> putRequest(Executor ctx) {
        return null;
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
                        path("ping", () ->
                                route(
                                        get(() -> route(
                                                complete("PONG GET!")
                                        )),
                                        post(() -> route(
                                                complete("PONG POST!")
                                        ))
                                )
                        ),
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
                        get(() -> complete("Unknown endpoint.")),
                        post(() -> complete("Unknown endpoint."))
                );
    }

}
