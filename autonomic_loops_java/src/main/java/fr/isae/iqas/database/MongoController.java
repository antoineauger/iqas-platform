package fr.isae.iqas.database;

import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import fr.isae.iqas.MainClass;
import fr.isae.iqas.model.virtualsensor.VirtualSensor;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by an.auger on 20/09/2016.
 */
public class MongoController extends AllDirectives {

    private MongoDatabase mongoDatabase;

    public MongoController(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }

    /**
     * GET all sensors
     * @param callback
     */

    public void _getAllSensors(final SingleResultCallback<List<VirtualSensor>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("sensors");
        collection.find().map((mydoc) -> new VirtualSensor(mydoc)).into(new ArrayList<VirtualSensor>(), callback);
    }

    public Route getAllSensors() {
        final CompletableFuture<List<VirtualSensor>> result = new CompletableFuture<>();
        _getAllSensors((sensors, t) -> {
            if (t == null) {
                result.complete(sensors);
            } else {
                result.completeExceptionally(t);
            }
        });
        return completeOKWithFuture(result, Jackson.<List<VirtualSensor>>marshaller());
    }

    /**
     * GET a specific sensor
     * @param sensor_id
     * @param callback
     */
    public void _getSensor(String sensor_id, final SingleResultCallback<List<VirtualSensor>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("sensors");
        collection.find(eq("sensor_id", sensor_id)).map((mydoc) -> new VirtualSensor(mydoc)).into(new ArrayList<VirtualSensor>(), callback);
    }

    public Route getSensor(String sensor_id) {
        final CompletableFuture<List<VirtualSensor>> result = new CompletableFuture<>();
        _getSensor(sensor_id, (sensors, t) -> {
            if (t == null) {
                result.complete(sensors);
            } else {
                result.completeExceptionally(t);
            }
        });
        return completeOKWithFuture(result, Jackson.<List<VirtualSensor>>marshaller());
    }

    // TODO remove method, only for testing
    public void putSensorsFromFileIntoDB(String sensorFileName, boolean blocking) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("sensors");
        List<Document> documents = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(MainClass.class.getClassLoader().getResourceAsStream(sensorFileName), StandardCharsets.UTF_8))) {
            String sCurrentLine;
            while ((sCurrentLine = reader.readLine()) != null) {
                documents.add(Document.parse(sCurrentLine));
            }
            collection.insertMany(documents, new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    System.out.println("Documents inserted!");
                }
            });
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
