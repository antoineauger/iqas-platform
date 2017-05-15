package fr.isae.iqas.database;

import akka.actor.ActorRef;
import akka.actor.UntypedActorContext;
import akka.dispatch.OnComplete;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import fr.isae.iqas.config.Config;
import fr.isae.iqas.model.message.PipelineRequestMsg;
import fr.isae.iqas.pipelines.Pipeline;
import fr.isae.iqas.utils.ActorUtils;
import ioinformarics.oss.jackson.module.jsonld.JsonldModule;
import ioinformarics.oss.jackson.module.jsonld.JsonldResource;
import ioinformarics.oss.jackson.module.jsonld.JsonldResourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 03/02/2017.
 */
public class FusekiRESTController extends AllDirectives {
    private Logger log = LoggerFactory.getLogger(FusekiRESTController.class);

    private String baseQoOIRI = null;
    private FusekiController controller = null;
    private UntypedActorContext context;
    private String pathPipelineWatcherActor;

    public FusekiRESTController(Config iqasConfig, UntypedActorContext context, String pathPipelineWatcherActor) {
        this.controller = new FusekiController(iqasConfig);
        this.baseQoOIRI = iqasConfig.getIRIForPrefix("qoo", false);
        this.context = context;
        this.pathPipelineWatcherActor = pathPipelineWatcherActor;
    }

    public FusekiController getController() {
        return controller;
    }

    /**
     * Sensors
     */

    public CompletableFuture<Route> getAllSensors(Executor ctx) {
        CompletableFuture<Route> sensors;
        ObjectMapper mapper = new ObjectMapper();

        sensors = CompletableFuture.supplyAsync(() -> controller.findAllSensors(), ctx).thenApply((result) -> {
            if (result == null) {
                return complete(HttpResponse.create()
                        .withStatus(400)
                        .withEntity("No sensor has been registered yet!"));
            }
            else {
                mapper.registerModule(new JsonldModule(mapper::createObjectNode));
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
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
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                JsonldResourceBuilder builder = JsonldResource.Builder.create();
                builder.context(baseQoOIRI);
                return completeOK(builder.build(result), Jackson.marshaller(mapper));
            }
        });

        return sensor;
    }

    /**
     * Topics
     */

    public CompletableFuture<Route> getAllTopics(Executor ctx) {
        CompletableFuture<Route> topics;
        ObjectMapper mapper = new ObjectMapper();

        topics = CompletableFuture.supplyAsync(() -> controller.findAllTopics(), ctx).thenApply((result) -> {
            if (result == null) {
                return complete(HttpResponse.create()
                        .withStatus(400)
                        .withEntity("No topic currently, come back later!"));
            }
            else {
                mapper.registerModule(new JsonldModule(mapper::createObjectNode));
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                JsonldResourceBuilder builder = JsonldResource.Builder.create();
                builder.context(baseQoOIRI);
                return completeOK(builder.build(result), Jackson.marshaller(mapper));
            }
        });

        return topics;
    }

    public CompletableFuture<Route> getSpecificTopic(String topic_id, Executor ctx) {
        CompletableFuture<Route> topic;
        ObjectMapper mapper = new ObjectMapper();

        topic = CompletableFuture.supplyAsync(() -> controller._findSpecificTopic(topic_id), ctx).thenApply((result) -> {
            if (result == null) {
                return complete(HttpResponse.create()
                        .withStatus(400)
                        .withEntity("Unable to find topic " + topic_id + "! " +
                                "Please check namespace and retry."));
            }
            else {
                mapper.registerModule(new JsonldModule(mapper::createObjectNode));
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                JsonldResourceBuilder builder = JsonldResource.Builder.create();
                builder.context(baseQoOIRI);
                return completeOK(builder.build(result), Jackson.marshaller(mapper));
            }
        });

        return topic;
    }

    /**
     * Pipelines
     */

    public CompletableFuture<Route> getConcretePipelineIDs() {
        CompletableFuture<Route> routeResponse;
        CompletableFuture<ArrayList<String>> pipelines = new CompletableFuture<>();
        ObjectMapper mapper = new ObjectMapper();

        ActorUtils.getPipelineWatcherActor(context, pathPipelineWatcherActor).onComplete(new OnComplete<ActorRef>() {
            @Override
            public void onComplete(Throwable t, ActorRef pipelineWatcherActor) throws Throwable {
                if (t != null) {
                    pipelines.completeExceptionally(
                            new Throwable("Unable to find the PipelineWatcherActor: " + t.toString())
                    );
                }
                else {
                    PipelineRequestMsg a = new PipelineRequestMsg();
                    ArrayList<String> arrayTemp = new ArrayList<>();
                    Timeout timeout = new Timeout(5, TimeUnit.SECONDS);
                    Future<Object> o = Patterns.ask(pipelineWatcherActor, a, timeout);
                    ArrayList<Pipeline> result = (ArrayList<Pipeline>) Await.result(o, timeout.duration());
                    for (Pipeline p : result) {
                        arrayTemp.add(p.getPipeline_id());
                    }
                    pipelines.complete(arrayTemp);
                }
            }
        }, context.dispatcher());

        routeResponse = pipelines.thenApply((result) -> {
            if (result.size() == 0) {
                return complete((HttpResponse.create()
                        .withStatus(400)
                        .withEntity("No pipeline has been found!")));
            }
            else {
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                return completeOKWithFuture(pipelines, Jackson.marshaller(mapper));
            }
        });

        return routeResponse;
    }

    public CompletableFuture<Route> getConcretePipelines() {
        CompletableFuture<Route> routeResponse;
        CompletableFuture<ArrayList<Pipeline>> pipelines = new CompletableFuture<>();
        ObjectMapper mapper = new ObjectMapper();

        ActorUtils.getPipelineWatcherActor(context, pathPipelineWatcherActor).onComplete(new OnComplete<ActorRef>() {
            @Override
            public void onComplete(Throwable t, ActorRef pipelineWatcherActor) throws Throwable {
                if (t != null) {
                    pipelines.completeExceptionally(
                            new Throwable("Unable to find the PipelineWatcherActor: " + t.toString())
                    );
                }
                else {
                    PipelineRequestMsg a = new PipelineRequestMsg();
                    Timeout timeout = new Timeout(5, TimeUnit.SECONDS);
                    Future<Object> o = Patterns.ask(pipelineWatcherActor, a, timeout);
                    ArrayList<Pipeline> result = (ArrayList<Pipeline>) Await.result(o, timeout.duration());
                    pipelines.complete(result);
                }
            }
        }, context.dispatcher());

        routeResponse = pipelines.thenApply((result) -> {
            if (result.size() == 0) {
                return complete((HttpResponse.create()
                        .withStatus(400)
                        .withEntity("No pipeline has been found!")));
            }
            else {
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                return completeOKWithFuture(pipelines, Jackson.marshaller(mapper));
            }
        });

        return routeResponse;
    }

    public CompletableFuture<Route> getConcretePipeline(String pipeline_id) {
        CompletableFuture<Route> routeResponse;
        CompletableFuture<ArrayList<Pipeline>> pipeline = new CompletableFuture<>();
        ObjectMapper mapper = new ObjectMapper();

        ActorUtils.getPipelineWatcherActor(context, pathPipelineWatcherActor).onComplete(new OnComplete<ActorRef>() {
            @Override
            public void onComplete(Throwable t, ActorRef pipelineWatcherActor) throws Throwable {
                if (t != null) {
                    pipeline.completeExceptionally(
                            new Throwable("Unable to find the PipelineWatcherActor: " + t.toString())
                    );
                }
                else {
                    PipelineRequestMsg a = new PipelineRequestMsg(pipeline_id);
                    Timeout timeout = new Timeout(5, TimeUnit.SECONDS);
                    Future<Object> o = Patterns.ask(pipelineWatcherActor, a, timeout);
                    ArrayList<Pipeline> result = (ArrayList<Pipeline>) Await.result(o, timeout.duration());
                    pipeline.complete(result);
                }
            }
        }, context.dispatcher());

        routeResponse = pipeline.thenApply((result) -> {
            if (result.size() == 0) {
                return complete((HttpResponse.create()
                        .withStatus(400)
                        .withEntity("No pipeline found with id \"" + pipeline_id + "\"")));
            }
            else {
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                return completeOKWithFuture(pipeline, Jackson.marshaller(mapper));
            }
        });

        return routeResponse;
    }

    /**
     * Places
     */

    public CompletableFuture<Route> getAllPlaces(Executor ctx) {
        CompletableFuture<Route> places;
        ObjectMapper mapper = new ObjectMapper();

        places = CompletableFuture.supplyAsync(() -> controller._findAllPlaces(), ctx).thenApply((result) -> {
            if (result == null) {
                return complete(HttpResponse.create()
                        .withStatus(400)
                        .withEntity("There is no sensor deployed on the field yet! Please try later..."));
            }
            else {
                mapper.registerModule(new JsonldModule(mapper::createObjectNode));
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                JsonldResourceBuilder builder = JsonldResource.Builder.create();
                builder.context(baseQoOIRI);
                return completeOK(builder.build(result), Jackson.marshaller(mapper));
            }
        });

        return places;
    }

    public CompletableFuture<Route> getPlacesNearTo(String location, Executor ctx) {
        CompletableFuture<Route> places;
        ObjectMapper mapper = new ObjectMapper();

        places = CompletableFuture.supplyAsync(() -> controller._findPlacesNearTo(location), ctx).thenApply((result) -> {
            if (result == null) {
                return complete(HttpResponse.create()
                        .withStatus(400)
                        .withEntity("There is no sensor deployed on the field near to " + location + " yet! Please try later..."));
            }
            else {
                mapper.registerModule(new JsonldModule(mapper::createObjectNode));
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                JsonldResourceBuilder builder = JsonldResource.Builder.create();
                builder.context(baseQoOIRI);
                return completeOK(builder.build(result), Jackson.marshaller(mapper));
            }
        });

        return places;
    }

    /**
     * QoO attributes
     */

    public CompletableFuture<Route> getAllQoOAttributes(Executor ctx) {
        CompletableFuture<Route> qooAttributes;
        ObjectMapper mapper = new ObjectMapper();

        qooAttributes = CompletableFuture.supplyAsync(() -> controller._findAllQoOAttributes(), ctx).thenApply((result) -> {
            if (result == null) {
                return complete(HttpResponse.create()
                        .withStatus(400)
                        .withEntity("No QoO attributes available, " +
                                "the iQAS platform may not be able to provide QoO support..."));
            }
            else {
                mapper.registerModule(new JsonldModule(mapper::createObjectNode));
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                JsonldResourceBuilder builder = JsonldResource.Builder.create();
                builder.context(baseQoOIRI);
                return completeOK(builder.build(result), Jackson.marshaller(mapper));
            }
        });

        return qooAttributes;
    }

    /**
     * QoO customizable parameters
     */

    public CompletableFuture<Route> getAllQoOCustomizableAttributes(Executor ctx) {
        CompletableFuture<Route> qooAttributes;
        ObjectMapper mapper = new ObjectMapper();

        qooAttributes = CompletableFuture.supplyAsync(() -> controller._findAllQoOCustomizableParameters(), ctx).thenApply((result) -> {
            if (result == null) {
                return complete(HttpResponse.create()
                        .withStatus(400)
                        .withEntity("No customizable QoO attributes have been found, " +
                                "the iQAS platform may not be able to provide QoO support..."));
            }
            else {
                mapper.registerModule(new JsonldModule(mapper::createObjectNode));
                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                JsonldResourceBuilder builder = JsonldResource.Builder.create();
                builder.context(baseQoOIRI);
                return completeOK(builder.build(result), Jackson.marshaller(mapper));
            }
        });

        return qooAttributes;
    }

}
