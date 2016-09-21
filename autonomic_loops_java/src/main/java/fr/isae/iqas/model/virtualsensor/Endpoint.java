package fr.isae.iqas.model.virtualsensor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.Document;

/**
 * Created by an.auger on 13/09/2016.
 */
public class Endpoint {
    private final String name;
    private final String endpoint;

    @JsonCreator
    public Endpoint(@JsonProperty("name") String name,
                    @JsonProperty("endpoint") String endpoint) {
        this.name = name;
        this.endpoint = endpoint;
    }

    public Endpoint(Document bsonDocument) {
        this.name = bsonDocument.getString("name");
        this.endpoint = bsonDocument.getString("endpoint");
    }

    public String getLongitude() {
        return name;
    }

    public String getLatitude() {
        return endpoint;
    }
}
