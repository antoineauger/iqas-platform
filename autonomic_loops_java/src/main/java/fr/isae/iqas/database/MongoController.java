package fr.isae.iqas.database;

import akka.http.javadsl.server.AllDirectives;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import fr.isae.iqas.MainClass;
import fr.isae.iqas.model.entity.old.VirtualSensorJSON;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.model.request.State;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by an.auger on 20/09/2016.
 */
public class MongoController extends AllDirectives {
    private Logger log = LoggerFactory.getLogger(MongoController.class);

    private MongoDatabase mongoDatabase = null;

    public MongoController(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
        log.info("MongoController successfully created!");
    }

    /**
     * Sensors
     */

    void _findSpecificSensor(String sensor_id, final SingleResultCallback<ArrayList<VirtualSensorJSON>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("sensors");
        collection.find(eq("sensor_id", sensor_id))
                .map(myDoc -> new VirtualSensorJSON(myDoc)).into(new ArrayList<>(), callback);
    }

    void _findAllSensors(final SingleResultCallback<ArrayList<VirtualSensorJSON>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("sensors");
        collection.find().map(myDoc -> new VirtualSensorJSON(myDoc)).into(new ArrayList<>(), callback);
    }

    /**
     * Requests
     */

    void _findSpecificRequest(String field, String value, final SingleResultCallback<ArrayList<Request>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("requests");
        collection.find(eq(field, value)).map(myDoc -> new Request(myDoc)).into(new ArrayList<>(), callback);
    }

    void _findAllRequests(final SingleResultCallback<ArrayList<Request>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("requests");
        collection.find().map(myDoc -> new Request(myDoc)).into(new ArrayList<>(), callback);
    }

    private void _putRequest(Document req, final SingleResultCallback<Void> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("requests");
        collection.insertOne(req, callback);
    }

    /**
     * Method to get all Requests for a specific application
     *
     * @param application_id String, the ID of the application
     * @return a CompletableFuture that will be completed with either an ArrayList of Requests or a Throwable
     */
    public CompletableFuture<ArrayList<Request>> getAllRequestsByApp(String application_id) {
        final CompletableFuture<ArrayList<Request>> requests = new CompletableFuture<>();
        _findSpecificRequest("application_id", application_id, (result, t) -> {
            if (t == null) {
                requests.complete(result);
            }
            else {
                requests.completeExceptionally(t);
            }
        });
        return requests;
    }

    /**
     * Method to get all Requests with specific statuses and for a specific application
     *
     * @param application_id String, the ID of the application
     * @param filters        an ArrayList of Status objects. Only return Requests with one of these statuses.
     * @return a CompletableFuture that will be completed with either an ArrayList of Requests or a Throwable
     */
    public CompletableFuture<ArrayList<Request>> getFilteredRequestsByApp(String application_id, ArrayList<State.Status> filters) {
        final CompletableFuture<ArrayList<Request>> requests = new CompletableFuture<>();
        _findSpecificRequest("application_id", application_id, (result, t) -> {
            if (t == null) {
                ArrayList<Request> requestTempList = result.stream()
                        .filter(r -> filters.contains(r.getCurrent_status()))
                        .collect(Collectors.toCollection(ArrayList::new));
                requests.complete(requestTempList);
            }
            else {
                requests.completeExceptionally(t);
            }
        });
        return requests;
    }

    /**
     * Method to get all Requests from database
     *
     * @return a CompletableFuture that will be completed with either an ArrayList of Requests or a Throwable
     */
    public CompletableFuture<ArrayList<Request>> getAllRequests() {
        final CompletableFuture<ArrayList<Request>> requests = new CompletableFuture<>();
        _findAllRequests((result, t) -> {
            if (t == null) {
                requests.complete(result);
            }
            else {
                requests.completeExceptionally(t);
            }
        });
        return requests;
    }


    /**
     * Method to insert a Request into database
     *
     * @param request the Request object to insert
     * @return a CompletableFuture that will be completed with a boolean denoting the success of the operation
     */
    public CompletableFuture<Boolean> putRequest(Request request) {
        final CompletableFuture<Boolean> insertedRequest = new CompletableFuture<>();
        _putRequest(request.toBSON(), (result, t) -> {
            if (t == null) {
                log.info("Successfully inserted Request " + request.getRequest_id() + " into requests collection!");
                insertedRequest.complete(true);
            }
            else {
                log.error("Unable to insert Request " + request.getRequest_id() + " into requests collection!");
                insertedRequest.complete(false);
            }
        });
        return insertedRequest;
    }

    // TODO : putRequests useful ?
    // ArrayList<Document> documents = requests.stream().map(Request::toBSON).collect(Collectors.toCollection(ArrayList::new));

    // ######### Internal mongoDB methods #########

    private void _dropIQASDatabase(final SingleResultCallback<Void> callback) {
        mongoDatabase.drop(callback);
    }

    /**
     * Method to drop the whole iQAS database
     */
    public void dropIQASDatabase() {
        _dropIQASDatabase((result, t) -> {
            if (t == null) {
                log.info("Drop of the iQAS database successful!");
            }
            else {
                log.error("Drop of the iQAS database failed: " + t.toString());
            }
        });
    }

    private void _dropCollection(String collectionName, final SingleResultCallback<Void> callback) {
        mongoDatabase.getCollection(collectionName).drop(callback);
    }

    /**
     * Method to drop a specific collection
     *
     * @param collectionName String name of the collection to drop
     */
    public void dropCollection(String collectionName) {
        _dropCollection(collectionName, (result, t) -> {
            if (t == null) {
                log.info("Drop of the " + collectionName + " collection successful!");
            }
            else {
                log.error("Drop of the " + collectionName + " collection failed: " + t.toString());
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
                    log.info("Sensors inserted into sensors collection!");
                }
                else {
                    log.error("Failed to insert sensors");
                }
            });
        } catch (Throwable t) {
            log.error("Unable to insert sensors into iQAS database: " + t.toString());
        }
    }
}
