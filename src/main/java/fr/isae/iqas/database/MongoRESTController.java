package fr.isae.iqas.database;

import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.async.client.MongoDatabase;
import fr.isae.iqas.model.request.Request;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Created by an.auger on 17/11/2016.
 */
public class MongoRESTController extends AllDirectives {
    private org.slf4j.Logger log = LoggerFactory.getLogger(MongoRESTController.class);

    private MongoController controller;

    public MongoRESTController(MongoDatabase mongoDatabase) {
        this.controller = new MongoController(mongoDatabase);
    }

    public MongoController getController() {
        return controller;
    }

    /**
     * Requests
     */

    /**
     * Method to get a specific Request from database
     *
     * @param request_id String, the request ID to retrieve
     * @param ctx
     * @return object Route (which contains either the Request or an error)
     */
    public CompletableFuture<Route> getRequest(String request_id, Executor ctx) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        final CompletableFuture<List<Request>> requests = new CompletableFuture<>();

        controller._findSpecificRequest("request_id", request_id, (result, t) -> {
            if (t == null) {
                requests.complete(result);
            }
            else {
                requests.completeExceptionally(t);
            }
        });
        return CompletableFuture.supplyAsync(() -> completeOKWithFuture(requests, Jackson.marshaller(mapper)), ctx);
    }

    /**
     * Method to get all Requests for a specific application
     *
     * @param application_id String, the ID of the application
     * @return object Route (which contains either the app Requests or an error)
     */
    public CompletableFuture<Route> getRequestsByApplication(String application_id) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        final CompletableFuture<List<Request>> requests = new CompletableFuture<>();

        controller._findSpecificRequest("application_id", application_id, (result, t) -> {
            if (t == null) {
                requests.complete(result);
            }
            else {
                requests.completeExceptionally(t);
            }
        });
        return CompletableFuture.supplyAsync(() -> completeOKWithFuture(requests, Jackson.marshaller(mapper)));
    }

    /**
     * Method to get all Requests from database
     *
     * @return object Route (which contains either all Requests or an error)
     * @param ctx
     */
    public CompletableFuture<Route> getAllRequests(Executor ctx) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        final CompletableFuture<List<Request>> requests = new CompletableFuture<>();

        controller._findAllRequests((result, t) -> {
            if (t == null) {
                requests.complete(result);
            }
            else {
                requests.completeExceptionally(t);
            }
        });
        return CompletableFuture.supplyAsync(() -> completeOKWithFuture(requests, Jackson.marshaller(mapper)), ctx);
    }
}
