package fr.isae.iqas.server;

import akka.actor.ActorRef;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import fr.isae.iqas.database.FusekiRESTController;
import fr.isae.iqas.database.MongoRESTController;
import fr.isae.iqas.model.message.RESTRequestMsg;
import fr.isae.iqas.model.request.Request;
import org.bson.types.ObjectId;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static akka.http.javadsl.server.PathMatchers.segment;
import static fr.isae.iqas.model.message.RESTRequestMsg.RequestSubject.*;

/**
 * Created by an.auger on 16/09/2016.
 */

public class RESTServer extends AllDirectives {
    private MongoRESTController mongoRESTController;
    private FusekiRESTController fusekiRESTController;
    private ActorRef apiGatewayActor;

    public RESTServer(MongoRESTController mongoRESTController, FusekiRESTController fusekiRESTController, ActorRef apiGatewayActor) {
        this.mongoRESTController = mongoRESTController;
        this.fusekiRESTController = fusekiRESTController;
        this.apiGatewayActor = apiGatewayActor;
    }

    /**
     * Fuseki controller: sensors, topics, places, QoO pipelines
     */

    private CompletionStage<Route> getSensor(Executor ctx, String sensor_id) {
        return fusekiRESTController.getSensor(sensor_id, ctx);
    }

    private CompletionStage<Route> getAllSensors(Executor ctx) {
        return fusekiRESTController.getAllSensors(ctx);
    }

    private CompletionStage<Route> getAllTopics(Executor ctx) {
        return fusekiRESTController.getAllTopics(ctx);
    }

    private CompletionStage<Route> getSpecificTopic(Executor ctx, String topic_id) {
        return fusekiRESTController.getSpecificTopic(topic_id, ctx);
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

    private CompletionStage<Route> getAllQoOAttributes(Executor ctx) {
        return fusekiRESTController.getAllQoOAttributes(ctx);
    }

    private CompletionStage<Route> getAllQoOCustomizableAttributes(Executor ctx) {
        return fusekiRESTController.getAllQoOCustomizableAttributes(ctx);
    }

    private CompletionStage<Route> getAllPlaces(Executor ctx) {
        return fusekiRESTController.getAllPlaces(ctx);
    }

    private CompletionStage<Route> getPlacesNearTo(Executor ctx, String location) {
        return fusekiRESTController.getPlacesNearTo(location, ctx);
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

    public Route createRoute() {
        return
                route(
                        get(() -> route(

                                // Homepage, images, css and scripts
                                pathSingleSlash(() ->
                                        getFromResource("web/index.html", ContentTypes.TEXT_HTML_UTF8)
                                ),
                                path(segment("style.css"), () ->
                                        getFromResource("web/style/style.css")
                                ),
                                path(segment("script.js"), () ->
                                        getFromResource("web/js/script.js")
                                ),
                                path(segment("iqas_logo.png"), () ->
                                        getFromResource("web/figures/iqas_logo.png")
                                ),

                                // REST APIs
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
                                ),
                                path(segment("topics"), () ->
                                        extractExecutionContext(ctx ->
                                                onSuccess(() -> getAllTopics(ctx), Function.identity())
                                        )
                                ),
                                path(segment("topics").slash(segment()), topic_id ->
                                        extractExecutionContext(ctx ->
                                                onSuccess(() -> getSpecificTopic(ctx, topic_id), Function.identity())
                                        )
                                ),
                                path(segment("places"), () -> parameterOptional("nearTo", locName -> {
                                            String location = locName.orElse("");
                                            if (location.equals("")) {
                                                return extractExecutionContext(ctx ->
                                                        onSuccess(() -> getAllPlaces(ctx), Function.identity())
                                                );
                                            }
                                            else {
                                                return extractExecutionContext(ctx ->
                                                        onSuccess(() -> getPlacesNearTo(ctx, location), Function.identity())
                                                );
                                            }})
                                ),

                                // QoO documentation
                                path(segment("qoo").slash(segment("custom_params")), () ->
                                        extractExecutionContext(ctx ->
                                                onSuccess(() -> getAllQoOCustomizableAttributes(ctx), Function.identity())
                                        )
                                ),
                                path(segment("qoo").slash(segment("iqas_params")), () ->
                                        getFromResource("web/iqas_params.html", ContentTypes.TEXT_HTML_UTF8)
                                ),
                                path(segment("qoo").slash(segment("attributes")), () ->
                                        extractExecutionContext(ctx ->
                                                onSuccess(() -> getAllQoOAttributes(ctx), Function.identity())
                                        )
                                )
                        )),
                        post(() -> route(
                                path(segment("requests"), () ->
                                        entity(Jackson.unmarshaller(Request.class), myRequest ->
                                                extractExecutionContext(ctx ->
                                                        onSuccess(() -> {
                                                            myRequest.setRequest_id(new ObjectId().toString()); // new random id for request identification
                                                            RESTRequestMsg m = new RESTRequestMsg(POST, myRequest); // construction of a special actor message
                                                            apiGatewayActor.tell(m, ActorRef.noSender()); // POST request forwarded to APIGatewayActor
                                                            return CompletableFuture.supplyAsync(() ->
                                                                    completeOK(myRequest, Jackson.marshaller()), ctx);
                                                        }, Function.identity())
                                                )
                                        ).orElse(
                                                complete(HttpResponse.create()
                                                        .withStatus(400)
                                                        .withEntity("Malformed request submitted!")
                                                )
                                        )
                                )
                        )),
                        delete(() -> route(
                                path(segment("requests").slash(segment()), request_id ->
                                        extractExecutionContext(ctx ->
                                                onSuccess(() -> {
                                                    RESTRequestMsg m = new RESTRequestMsg(DELETE, new Request(request_id)); // construction of a special actor message
                                                    apiGatewayActor.tell(m, ActorRef.noSender()); // POST request forwarded to APIGatewayActor
                                                    return CompletableFuture.supplyAsync(() ->
                                                            complete(HttpResponse.create()
                                                                    .withStatus(200)
                                                                    .withEntity("Asked for the deletion of request " + request_id)));
                                                }, Function.identity())

                                        ))
                        )),

                        // Error handling
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
                        ),
                        delete(() -> complete(
                                HttpResponse.create()
                                        .withStatus(404)
                                        .withEntity("Unknown API endpoint!"))
                        )
                );
    }

}
