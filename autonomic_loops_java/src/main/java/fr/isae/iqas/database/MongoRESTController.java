package fr.isae.iqas.database;

import akka.actor.ActorRef;
import akka.actor.UntypedActorContext;
import akka.dispatch.OnComplete;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import akka.util.Timeout;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.model.virtualsensor.VirtualSensor;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import scala.concurrent.Future;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 17/11/2016.
 */
public class MongoRESTController extends AllDirectives {
    private static Logger log = Logger.getLogger(MongoController.class);

    private MongoController controller;
    private MongoDatabase mongoDatabase;
    private UntypedActorContext context;
    private String pathAPIGatewayActor;

    public MongoRESTController(MongoDatabase mongoDatabase, UntypedActorContext context, String pathAPIGatewayActor) {
        this.controller = new MongoController(mongoDatabase);
        this.mongoDatabase = mongoDatabase;
        this.context = context;
        this.pathAPIGatewayActor = pathAPIGatewayActor;
    }

    public MongoController getController() {
        return controller;
    }

    private Future<ActorRef> getAPIGatewayActor() {
        return context.actorSelection(pathAPIGatewayActor).resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

    /**
     * Sensors
     */

    public Route getAllSensors() {
        final CompletableFuture<ArrayList<VirtualSensor>> sensors = new CompletableFuture<>();
        controller._findAllSensors((result, t) -> {
            if (t == null) {
                sensors.complete(result);
            } else {
                sensors.completeExceptionally(t);
            }
        });
        return completeOKWithFuture(sensors, Jackson.marshaller());
    }

    public Route getSensor(String sensor_id) {
        final CompletableFuture<ArrayList<VirtualSensor>> sensor = new CompletableFuture<>();
        controller._findSpecificSensor(sensor_id, (result, t) -> {
            if (t == null) {
                sensor.complete(result);
            } else {
                sensor.completeExceptionally(t);
            }
        });
        return completeOKWithFuture(sensor, Jackson.marshaller());
    }

    /**
     * Requests
     */

    /**
     * Method to get a specific Request from database
     * @param request_id: String, the request ID to retrieve
     * @return object Route (which contains either the Request or an error)
     */
    public Route getRequest(String request_id) {
        final CompletableFuture<ArrayList<Request>> requests = new CompletableFuture<>();
        controller._findSpecificRequest("request_id", request_id, (result, t) -> {
            if (t == null) {
                requests.complete(result);
            } else {
                requests.completeExceptionally(t);
            }
        });
        return completeOKWithFuture(requests, Jackson.marshaller());
    }

    /**
     * Method to get all Requests for a specific application
     * @param application_id: String, the ID of the application
     * @return object Route (which contains either the app Requests or an error)
     */
    public Route getRequestsByApplication(String application_id) {
        final CompletableFuture<ArrayList<Request>> requests = new CompletableFuture<>();
        controller._findSpecificRequest("application_id", application_id, (result, t) -> {
            if (t == null) {
                requests.complete(result);
            } else {
                requests.completeExceptionally(t);
            }
        });
        return completeOKWithFuture(requests, Jackson.marshaller());
    }

    /**
     * Method to get all Requests from database
     * @return object Route (which contains either all Requests or an error)
     */
    public Route getAllRequests() {
        final CompletableFuture<ArrayList<Request>> requests = new CompletableFuture<>();
        controller._findAllRequests((result, t) -> {
            if (t == null) {
                requests.complete(result);
            } else {
                requests.completeExceptionally(t);
            }
        });
        return completeOKWithFuture(requests, Jackson.marshaller());
    }

    /**
     * Method to submit a new observation Request
     * This method will:
     * 1) Insert it into mongoDB
     * 2) Tell APIGatewayActor that a new request should be taken into account
     * @param request the Request object to insert into mongoDB
     * @return object Request with mongoDB _id to check request processing
     */
    public Route putRequest(Request request) {
        // request_id assignment
        request.setRequest_id(new ObjectId().toString());

        CompletableFuture<Request> requestResultInsertion = new CompletableFuture<>();
        MongoCollection<Document> collection = mongoDatabase.getCollection("requests");

        Document documentRequest = request.toBSON();
        collection.insertOne(documentRequest, (result, t) -> {
            if (t == null) {
                log.info("Successfully inserted Requests into requests collection!");

                getAPIGatewayActor().onComplete(new OnComplete<ActorRef>() {
                    @Override
                    public void onComplete(Throwable failure, ActorRef success) throws Throwable {
                        if (failure != null) {
                            requestResultInsertion.completeExceptionally(
                                    new Throwable("Unable to find the APIGatewayActor: " + failure.toString())
                            );
                        } else {
                            success.tell(request, ActorRef.noSender());
                            log.info("Request sent to the APIGatewayActor by MongoController");
                            requestResultInsertion.complete(request);
                        }
                    }
                }, context.dispatcher());

            } else {
                log.info("Failed to insert request: " + t.toString());
            }
        });
        return completeOKWithFuture(requestResultInsertion, Jackson.marshaller());
    }
}
