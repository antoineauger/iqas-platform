package fr.isae.iqas.model.virtualsensor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class VirtualSensor {
    private final String sensor_id;
    private final String type;
    private final String location_name;
    private final List<Coordinates> coordinates;
    private final List<Endpoint> topics;

    @JsonCreator
    public VirtualSensor(@JsonProperty("sensor_id") String sensor_id,
                         @JsonProperty("type") String type,
                         @JsonProperty("location_name") String location_name,
                         @JsonProperty("coordinates") List<Coordinates> coordinates,
                         @JsonProperty("topics") List<Endpoint> topics) {
        this.sensor_id = sensor_id;
        this.type = type;
        this.location_name = location_name;
        this.coordinates = coordinates;
        this.topics = topics;
    }

    public VirtualSensor(Document bsonDocument) {
        this.sensor_id = bsonDocument.getString("sensor_id");
        this.type = bsonDocument.getString("type");
        this.location_name = bsonDocument.getString("location_name");
        this.coordinates = new ArrayList<>();

        List<Document> coordinatesDocument = (List<Document>) bsonDocument.get("coordinates");
        Coordinates coordinatesTemp = new Coordinates(coordinatesDocument.get(0));
        this.coordinates.add(coordinatesTemp);

        List<Document> topicsDocument = (List<Document>) bsonDocument.get("topics");
        List<Endpoint> topicsToAdd = new ArrayList<>();
        for (Document doc : topicsDocument) {
            topicsToAdd.add(new Endpoint(doc));
        }
        this.topics = topicsToAdd;
    }

    public String getSensor_id() {
        return sensor_id;
    }

    public String getType() {
        return type;
    }

    public String getLocation_name() {
        return location_name;
    }

    public List<Coordinates> getCoordinates() {
        return coordinates;
    }

    public List<Endpoint> getTopics() {
        return topics;
    }
}
