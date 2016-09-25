package fr.isae.iqas.database;

import akka.actor.ActorRef;
import akka.actor.UntypedActorContext;
import akka.dispatch.OnComplete;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import akka.util.Timeout;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import fr.isae.iqas.MainClass;
import fr.isae.iqas.model.Request;
import fr.isae.iqas.model.virtualsensor.VirtualSensor;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import scala.concurrent.Future;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by an.auger on 20/09/2016.
 */
public class MongoController extends AllDirectives {
    private static Logger logger = Logger.getLogger(MongoController.class);

    private String pathAPIGatewayActor;
    private MongoDatabase mongoDatabase;
    private UntypedActorContext context;

    public MongoController(MongoDatabase mongoDatabase, UntypedActorContext context, String pathAPIGatewayActor) {
        this.pathAPIGatewayActor = pathAPIGatewayActor;
        this.mongoDatabase = mongoDatabase;
        this.context = context;
        logger.info("MongoController successfully created!");
    }

    public Future<ActorRef> getAPIGatewayActor() {
        return context.actorSelection(pathAPIGatewayActor).
                resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

    // ######### Exposed mongoDB methods #########

    /**
     * Sensors
     */

    private void _findSensors(String sensor_id, final SingleResultCallback<List<VirtualSensor>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("sensors");
        if (sensor_id != null) {
            collection.find(eq("sensor_id", sensor_id)).map((mydoc) -> new VirtualSensor(mydoc)).into(new ArrayList<>(), callback);
        } else {
            collection.find().map((mydoc) -> new VirtualSensor(mydoc)).into(new ArrayList<>(), callback);
        }
    }

    public Route getAllSensors() {
        final CompletableFuture<List<VirtualSensor>> sensors = new CompletableFuture<>();
        _findSensors(null, (result, t) -> {
            if (t == null) {
                sensors.complete(result);
            } else {
                sensors.completeExceptionally(t);
            }
        });
        return completeOKWithFuture(sensors, Jackson.marshaller());
    }

    public Route getSensor(String sensor_id) {
        final CompletableFuture<List<VirtualSensor>> sensor = new CompletableFuture<>();
        _findSensors(sensor_id, (result, t) -> {
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

    private void _findRequests(String request_id, final SingleResultCallback<List<Request>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("requests");
        if (request_id != null) {
            collection.find(eq("request_id", request_id)).map((mydoc) -> new Request(mydoc)).into(new ArrayList<>(), callback);
        } else {
            collection.find().map((mydoc) -> new Request(mydoc)).into(new ArrayList<>(), callback);
        }
    }

    public Route getAllRequests() {
        final CompletableFuture<List<Request>> requests = new CompletableFuture<>();
        _findRequests(null, (result, t) -> {
            if (t == null) {
                requests.complete(result);
            } else {
                requests.completeExceptionally(t);
            }
        });
        return completeOKWithFuture(requests, Jackson.marshaller());
    }

    public Route getRequest(String request_id) {
        final CompletableFuture<List<Request>> requests = new CompletableFuture<>();
        _findRequests(request_id, (result, t) -> {
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
     * 1) TODO: Check if there is no existing method with the same parameters for the same application
     * 2) If not, insert it into mongoDB
     * 3) Tell APIGatewayActor that a new request should be taken into account
     *
     * @param request the request to insert into mongoDB
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
                logger.info("Successfully inserted Requests into requests collection!");

                getAPIGatewayActor().onComplete(new OnComplete<ActorRef>() {
                    @Override
                    public void onComplete(Throwable failure, ActorRef success) throws Throwable {
                        if (failure != null) {
                            requestResultInsertion.completeExceptionally(
                                    new Throwable("Unable to find the APIGatewayActor: " + failure.toString())
                            );
                        } else {
                            success.tell(request, ActorRef.noSender());
                            logger.info("Request sent to the APIGatewayActor by MongoController");
                            requestResultInsertion.complete(request);
                        }
                    }
                }, context.dispatcher());

            } else {
                logger.info("Failed to insert request: " + t.toString());
            }
        });
        return completeOKWithFuture(requestResultInsertion, Jackson.marshaller());
    }

    // TODO : putRequests useful ?
    // ArrayList<Document> documents = requests.stream().map(Request::toBSON).collect(Collectors.toCollection(ArrayList::new));

    // ######### Internal mongoDB methods #########

    private void _dropIQASDatabase(final SingleResultCallback<Void> callback) {
        mongoDatabase.drop(callback);
    }

    public void dropIQASDatabase() {
        _dropIQASDatabase((result, t) -> {
            if (t == null) {
                logger.info("Drop of the IQAS database successful!");
            } else {
                logger.info("Drop of the IQAS database failed: " + t.toString());
            }
        });
    }

    private void _dropCollection(String collectionName, final SingleResultCallback<Void> callback) {
        mongoDatabase.getCollection(collectionName).drop(callback);
    }

    public void dropCollection(String collectionName) {
        _dropCollection(collectionName, (result, t) -> {
            if (t == null) {
                logger.info("Drop of the " + collectionName + " collection successful!");
            } else {
                logger.info("Drop of the " + collectionName + " collection failed: " + t.toString());
            }
        });
    }

    // ######### MongoDB methods for testing/development #########

    // TODO remove method, only for testing
    public void putSensorsFromFileIntoDB(String sensorFileName) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("sensors");
        List<Document> documents = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(MainClass.class.getClassLoader().getResourceAsStream(sensorFileName),
                        StandardCharsets.UTF_8))) {
            String sCurrentLine;
            while ((sCurrentLine = reader.readLine()) != null) {
                documents.add(Document.parse(sCurrentLine));
            }
            collection.insertMany(documents, (result, t) -> {

                if (t == null) {
                    logger.info("Sensors inserted into sensors collection!");
                } else {
                    logger.info("Failed to insert sensors");
                }
            });
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
