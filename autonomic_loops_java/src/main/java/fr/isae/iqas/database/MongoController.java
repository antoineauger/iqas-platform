package fr.isae.iqas.database;

import akka.http.javadsl.server.AllDirectives;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import fr.isae.iqas.MainClass;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.model.request.Status;
import fr.isae.iqas.model.virtualsensor.VirtualSensor;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by an.auger on 20/09/2016.
 */
public class MongoController extends AllDirectives {
    private static Logger log = Logger.getLogger(MongoController.class);

    private MongoDatabase mongoDatabase;

    MongoController(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
        log.info("MongoController successfully created!");
    }

    /**
     * Sensors
     */

    void _findSpecificSensor(String sensor_id, final SingleResultCallback<ArrayList<VirtualSensor>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("sensors");
        collection.find(eq("sensor_id", sensor_id)).map(myDoc -> new VirtualSensor(myDoc)).into(new ArrayList<>(), callback);
    }

    void _findAllSensors(final SingleResultCallback<ArrayList<VirtualSensor>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("sensors");
        collection.find().map(myDoc -> new VirtualSensor(myDoc)).into(new ArrayList<>(), callback);
    }

    /**
     * Requests
     */

    void _findSpecificRequest(String field, String value, final SingleResultCallback<ArrayList<Request>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("requests");
        collection.find(eq(field, value)).map((mydoc) -> new Request(mydoc)).into(new ArrayList<>(), callback);
    }

    void _findAllRequests(final SingleResultCallback<ArrayList<Request>> callback) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("requests");
        collection.find().map((mydoc) -> new Request(mydoc)).into(new ArrayList<>(), callback);
    }

    /**
     * Method to get all Requests for a specific application
     *
     * @param application_id String, the ID of the application
     * @return an ArrayList of Requests or an empty ArrayList
     */
    public ArrayList<Request> getAllRequestsByApplication(String application_id) {
        final ArrayList<Request> objectToReturn = new ArrayList<>();
        _findSpecificRequest("application_id", application_id, (result, t) -> {
            if (t == null) {
                objectToReturn.addAll(result);
            }
        });
        return objectToReturn;
    }

    /**
     * Method to get all Requests with specific statuses and for a specific application
     *
     * @param application_id String, the ID of the application
     * @param filters        an ArrayList of Status objects. Only return Requests with one of these statuses.
     * @return an ArrayList of Requests or an empty ArrayList
     */
    public ArrayList<Request> getAllRequestsWithFilterByApplication(String application_id, ArrayList<Status> filters) {
        final ArrayList<Request> objectToReturn = new ArrayList<>();
        _findSpecificRequest("application_id", application_id, (result, t) -> {
            if (t == null) {
                for (Request r : result) {
                    if (filters.contains(r.getCurrentStatus())) {
                        objectToReturn.add(r);
                    }
                }
            }
        });
        return objectToReturn;
    }

    /**
     * Method to get all Requests from database
     *
     * @return object Route (which contains either all Requests or an error)
     */
    public ArrayList<Request> getAllRequests() {
        final ArrayList<Request> objectToReturn = new ArrayList<>();
        _findAllRequests((result, t) -> {
            if (t == null) {
                objectToReturn.addAll(result);
            }
        });
        return objectToReturn;
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
                log.info("Drop of the iQAS database failed: " + t.toString());
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
                log.info("Drop of the " + collectionName + " collection failed: " + t.toString());
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
                    log.info("Failed to insert sensors");
                }
            });
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
