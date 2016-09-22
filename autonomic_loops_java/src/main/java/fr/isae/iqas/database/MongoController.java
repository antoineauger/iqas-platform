package fr.isae.iqas.database;

import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import fr.isae.iqas.MainClass;
import fr.isae.iqas.model.Request;
import fr.isae.iqas.model.virtualsensor.VirtualSensor;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.log4j.Logger;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by an.auger on 20/09/2016.
 */
public class MongoController extends AllDirectives {
    private static Logger logger = Logger.getLogger(MongoController.class);

    private MongoDatabase mongoDatabase;

    public MongoController(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }

    // ######### Exposed mongoDB methods #########

    /**
     * GET sensors
     */

    public void _findSensors(String sensor_id, final SingleResultCallback<List<VirtualSensor>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("sensors");
        if (sensor_id != null) {
            collection.find(eq("sensor_id", sensor_id)).map((mydoc) -> new VirtualSensor(mydoc)).into(new ArrayList<>(), callback);
        }
        else {
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
     * GET requests
     */
    public void _findRequests(String request_id, final SingleResultCallback<List<Request>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("requests");
        if (request_id != null) {
            collection.find(eq("request_id", request_id)).map((mydoc) -> new Request(mydoc)).into(new ArrayList<>(), callback);
        }
        else {
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

    // ######### Internal mongoDB methods #########

    public void _dropIQASDatabase(final SingleResultCallback<Void> callback) {
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

    public void _dropCollection(String collectionName, final SingleResultCallback<Void> callback) {
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
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(MainClass.class.getClassLoader().getResourceAsStream(sensorFileName), StandardCharsets.UTF_8))) {
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
