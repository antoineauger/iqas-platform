package fr.isae.iqas.model.virtualsensor.old;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.Document;

/**
 * Created by an.auger on 13/09/2016.
 */
public class CoordinatesJSON {
    private final double longitude;
    private final double latitude;

    @JsonCreator
    public CoordinatesJSON(@JsonProperty("x") double longitude,
                           @JsonProperty("y") double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public CoordinatesJSON(Document bsonDocument) {
        this.longitude = bsonDocument.getDouble("x");
        this.latitude = bsonDocument.getDouble("y");
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }
}
