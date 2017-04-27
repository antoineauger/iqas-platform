package fr.isae.iqas.database;

import akka.http.javadsl.server.AllDirectives;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import fr.isae.iqas.MainClass;
import fr.isae.iqas.kafka.RequestMapping;
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
        log.info("MongoController successfully created");
    }

    // ######### Internal mongoDB methods #########

    /**
     * MongoDB
     */

    private void _dropIQASDatabase(final SingleResultCallback<Void> callback) {
        mongoDatabase.drop(callback);
    }

    private void _dropCollection(String collectionName, final SingleResultCallback<Void> callback) {
        mongoDatabase.getCollection(collectionName).drop(callback);
    }

    /**
     * Requests
     */

    void _findSpecificRequest(String field, String value, final SingleResultCallback<List<Request>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("requests");
        collection.find(eq(field, value)).map(myDoc -> new Request(myDoc)).into(new ArrayList<>(), callback);
    }

    void _findAllRequests(final SingleResultCallback<List<Request>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("requests");
        collection.find().map(myDoc -> new Request(myDoc)).into(new ArrayList<>(), callback);
    }

    void _putRequest(Document req, final SingleResultCallback<Void> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("requests");
        collection.insertOne(req, callback);
    }

    void _updateRequest(String request_id, Document newReq, final SingleResultCallback<UpdateResult> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("requests");
        collection.replaceOne(eq("request_id", request_id), newReq, callback);
    }

    void _deleteRequest(String request_id, final SingleResultCallback<DeleteResult> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("requests");
        collection.deleteOne(eq("request_id", request_id), callback);
    }

    /**
     * Request Mappings
     */

    void _findAllRequestMappings(final SingleResultCallback<List<RequestMapping>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("request_mappings");
        collection.find().map(myDoc -> new RequestMapping(myDoc)).into(new ArrayList<>(), callback);
    }

    void _putRequestMapping(Document req, final SingleResultCallback<Void> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("request_mappings");
        collection.insertOne(req, callback);
    }

    void _updateRequestMapping(String associated_request_id, Document newReq, final SingleResultCallback<UpdateResult> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("request_mappings");
        collection.replaceOne(eq("request_id", associated_request_id), newReq, callback);
    }

    void _deleteRequestMapping(String associated_request_id, final SingleResultCallback<DeleteResult> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("request_mappings");
        collection.deleteOne(eq("request_id", associated_request_id), callback);
    }

    // ######### Exposed mongoDB methods #########

    public CompletableFuture<Boolean> deleteRequest(String request_id) {
        final CompletableFuture<Boolean> deletedRequest = new CompletableFuture<>();
        _deleteRequest(request_id, new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult result, final Throwable t) {
                if (result.getDeletedCount() == 1) {
                    deletedRequest.complete(true);
                }
                else {
                    log.error("Unable to delete request " + request_id + ": " + t.toString());
                    deletedRequest.complete(false);
                }
            }
        });
        return deletedRequest;
    }

    /**
     * Method to get all Requests for a specific application
     *
     * @param application_id String, the ID of the application
     * @return a CompletableFuture that will be completed with either a list of Requests or a Throwable
     */
    public CompletableFuture<List<Request>> getAllRequestsByApp(String application_id) {
        final CompletableFuture<List<Request>> requests = new CompletableFuture<>();
        _findSpecificRequest("application_id", application_id, (result, t) -> {
            if (t == null) {
                requests.complete(result);
            }
            else {
                log.error("Unable to retrieve all requests: " + t.toString());
                requests.completeExceptionally(t);
            }
        });
        return requests;
    }

    /**
     * Method to get all Requests with specific statuses and for a specific application
     *
     * @param application_id String, the ID of the application
     * @param filters        an list of Status objects. Only return Requests with one of these statuses.
     * @return a CompletableFuture that will be completed with either an list of Requests or a Throwable
     */
    public CompletableFuture<List<Request>> getFilteredRequestsByApp(String application_id, List<State.Status> filters) {
        final CompletableFuture<List<Request>> requests = new CompletableFuture<>();
        _findSpecificRequest("application_id", application_id, (result, t) -> {
            if (t == null) {
                List<Request> requestTempList = result.stream()
                        .filter(r -> filters.contains(r.getCurrent_status()))
                        .collect(Collectors.toCollection(ArrayList::new));
                requests.complete(requestTempList);
            }
            else {
                log.error("Unable to retrieve requests by application: " + t.toString());
                requests.completeExceptionally(t);
            }
        });
        return requests;
    }

    /**
     *
     *
     * @param request_id
     * @return
     */
    public CompletableFuture<List<Request>> getSpecificRequest(String request_id) {
        final CompletableFuture<List<Request>> requests = new CompletableFuture<>();
        _findSpecificRequest("request_id", request_id, (result, t) -> {
            if (t == null) {
                List<Request> requestTempList = result.stream()
                        .collect(Collectors.toCollection(ArrayList::new));
                requests.complete(requestTempList);
            }
            else {
                log.error("Unable to retrieve specific request: " + t.toString());
                requests.completeExceptionally(t);
            }
        });
        return requests;
    }

    /**
     * Method to get similar requests with same topic / location / qooConstraints
     *
     * @param request
     * @return
     */
    public CompletableFuture<List<Request>> getExactSameRequestsAs(Request request) {
        final CompletableFuture<List<Request>> requests = new CompletableFuture<>();
        _findAllRequests((result, throwable) -> {
            if (throwable == null) {
                List<Request> requestTempList = new ArrayList<>();
                for (Request r : result) {
                    if (!r.getRequest_id().equals(request.getRequest_id()) && r.equals(request) && r.isInState(State.Status.ENFORCED)) {
                        requestTempList.add(r);
                        break;
                    }
                }
                requests.complete(requestTempList);
            }
            else {
                log.error("Unable to retrieve similar requests: " + throwable.toString());
                requests.completeExceptionally(throwable);
            }
        });
        return requests;
    }

    /**
     * Method to get all Requests from database
     *
     * @return a CompletableFuture that will be completed with either an list of Requests or a Throwable
     */
    public CompletableFuture<List<Request>> getAllRequests() {
        final CompletableFuture<List<Request>> requests = new CompletableFuture<>();
        _findAllRequests((result, t) -> {
            if (t == null) {
                requests.complete(result);
            }
            else {
                log.error("Unable to retrieve all requests: " + t.toString());
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
                log.info("Successfully inserted Request " + request.getRequest_id() + " into requests collection");
                insertedRequest.complete(true);
            }
            else {
                log.error("Unable to insert Request " + request.getRequest_id() + " into requests collection");
                insertedRequest.complete(false);
            }
        });
        return insertedRequest;
    }

    /**
     *
     * @param request_id
     * @param newRequest
     * @return
     */
    public CompletableFuture<Boolean> updateRequest(String request_id, Request newRequest) {
        final CompletableFuture<Boolean> updatedRequest = new CompletableFuture<>();
        _updateRequest(request_id, newRequest.toBSON(), (result, t) -> {
            if (t == null) {
                log.info("Successfully updated Request " + request_id + "");
                updatedRequest.complete(true);
            }
            else {
                log.error("Unable to update Request " + request_id + ": " + t.toString());
                updatedRequest.complete(false);
            }
        });
        return updatedRequest;
    }

    // TODO : putRequests useful ?
    // List<Document> documents = requests.stream().map(Request::toBSON).collect(Collectors.toCollection(ArrayList::new));

    public CompletableFuture<Boolean> putRequestMapping(RequestMapping request) {
        final CompletableFuture<Boolean> insertedRequest = new CompletableFuture<>();
        _putRequestMapping(request.toBSON(), (result, t) -> {
            if (t == null) {
                insertedRequest.complete(true);
            }
            else {
                log.error("Error when inserting RequestMapping into database: " + t.toString());
                insertedRequest.complete(false);
            }
        });
        return insertedRequest;
    }

    public CompletableFuture<List<RequestMapping>> getSpecificRequestMapping(String request_id) {
        final CompletableFuture<List<RequestMapping>> requestMappings = new CompletableFuture<>();
        _findAllRequestMappings((result, t) -> {
            if (t == null) {
                List<RequestMapping> requestTempList = new ArrayList<>();
                for (RequestMapping r : result) {
                    if (r.getRequest_id().contains(request_id)) {
                        requestTempList.add(r);
                    }
                }
                requestMappings.complete(requestTempList);
            }
            else {
                log.error("Error when fetching Request Mapping concerning request " + request_id + ": " + t.toString());
                requestMappings.completeExceptionally(t);
            }
        });
        return requestMappings;
    }

    public CompletableFuture<Boolean> updateRequestMapping(String associated_request_id, RequestMapping newRequestMapping) {
        final CompletableFuture<Boolean> updatedRequest = new CompletableFuture<>();
        _updateRequestMapping(associated_request_id, newRequestMapping.toBSON(), (result, t) -> {
            if (t == null) {
                log.info("Successfully updated RequestMapping for request " + associated_request_id + "");
                updatedRequest.complete(true);
            }
            else {
                log.error("Unable to update RequestMapping for request " + associated_request_id + ": " + t.toString());
                updatedRequest.complete(false);
            }
        });
        return updatedRequest;
    }

    public CompletableFuture<Boolean> deleteRequestMapping(String request_id) {
        final CompletableFuture<Boolean> deletedRequest = new CompletableFuture<>();
        _deleteRequestMapping(request_id, new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult result, final Throwable t) {
                if (result.getDeletedCount() == 1) {
                    deletedRequest.complete(true);
                }
                else {
                    log.error("Unable to delete RequestMapping for request " + request_id + ": " + t.toString());
                    deletedRequest.complete(false);
                }
            }
        });
        return deletedRequest;
    }

    /**
     * Method to drop the whole iQAS database
     */
    public void dropIQASDatabase() {
        _dropIQASDatabase((result, t) -> {
            if (t == null) {
                log.info("Drop of the iQAS database successful");
            }
            else {
                log.error("Drop of the iQAS database failed: " + t.toString());
            }
        });
    }

    /**
     * Method to drop a specific collection
     *
     * @param collectionName String name of the collection to drop
     */
    public void dropCollection(String collectionName) {
        _dropCollection(collectionName, (result, t) -> {
            if (t == null) {
                log.info("Drop of the " + collectionName + " collection successful");
            }
            else {
                log.error("Drop of the " + collectionName + " collection failed: " + t.toString());
            }
        });
    }
}
