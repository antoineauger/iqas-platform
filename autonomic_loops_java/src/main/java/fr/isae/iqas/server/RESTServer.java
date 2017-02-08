package fr.isae.iqas.server;

import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import fr.isae.iqas.database.FusekiRESTController;
import fr.isae.iqas.database.MongoRESTController;
import fr.isae.iqas.model.request.Request;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static akka.http.javadsl.server.PathMatchers.segment;

/**
 * Created by an.auger on 16/09/2016.
 */

public class RESTServer extends AllDirectives {
    private MongoRESTController mongoRESTController;
    private FusekiRESTController fusekiRESTController;

    public RESTServer(MongoRESTController mongoRESTController, FusekiRESTController fusekiRESTController) {
        this.mongoRESTController = mongoRESTController;
        this.fusekiRESTController = fusekiRESTController;
    }

    /**
     * Fuseki controller: sensors, QoO pipelines
     */

    private CompletionStage<Route> getSensor(Executor ctx, String sensor_id) {
        return fusekiRESTController.getSensor(sensor_id, ctx);
    }

    private CompletionStage<Route> getAllSensors(Executor ctx) {
        return fusekiRESTController.getAllSensors(ctx);
    }

    private CompletionStage<Route> getAllConcretePipelineIDs() {
        return fusekiRESTController.getConcretePipelineIDs();
    }

    private CompletionStage<Route> getConcretePipeline(String pipeline_id) {
        return fusekiRESTController.getConcretePipeline(pipeline_id);
    }

    private CompletionStage<Route> getAllConcretePipelines() {
        return fusekiRESTController.getConcretePipelines();
    }

    /**
     * MongoDB controller: requests, MAPE-K logging
     */

    private CompletionStage<Route> getRequest(Executor ctx, String request_id) {
        return mongoRESTController.getRequest(request_id, ctx);
    }

    private CompletionStage<Route> getAllRequests(Executor ctx) {
        return mongoRESTController.getAllRequests(ctx);
    }

    private CompletionStage<Route> putRequest(Executor ctx, Request request) {
        return mongoRESTController.forwardRequestToAPIGateway(request, ctx);
    }

    public Route createRoute() {
        return
                route(
                        get(() -> route(
                                // matches the empty path
                                pathSingleSlash(() ->
                                        // homepage
                                        getFromResource("web/index.html", ContentTypes.TEXT_HTML_UTF8)
                                ),
                                path(segment("sensors"), () ->
                                        extractExecutionContext(ctx ->
                                                onSuccess(() -> getAllSensors(ctx), Function.identity())
                                        )
                                ),
                                path(segment("sensors").slash(segment()), sensor_id ->
                                        extractExecutionContext(ctx ->
                                                onSuccess(() -> getSensor(ctx, sensor_id), Function.identity())
                                        )
                                ),
                                path(segment("pipelines").slash(segment()), pipeline_id ->
                                        extractExecutionContext(ctx ->
                                                onSuccess(() -> getConcretePipeline(pipeline_id), Function.identity())
                                        )
                                ),
                                path(segment("pipelines"), () -> parameterOptional("print", optName -> {
                                        String print = optName.orElse("");
                                        if (print.equals("ids")) {
                                            return extractExecutionContext(ctx ->
                                                    onSuccess(() -> getAllConcretePipelineIDs(), Function.identity())
                                            );
                                        }
                                        else {
                                            return extractExecutionContext(ctx ->
                                                    onSuccess(() -> getAllConcretePipelines(), Function.identity())
                                            );
                                        }})
                                ),
                                path(segment("requests"), () ->
                                        extractExecutionContext(ctx ->
                                                onSuccess(() -> getAllRequests(ctx), Function.identity())
                                        )
                                ),
                                path(segment("requests").slash(segment()), request_id ->
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
