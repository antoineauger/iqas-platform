package fr.isae.iqas.model.entity.old;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.Document;

/**
 * Created by an.auger on 13/09/2016.
 */
public class EndpointJSON {
    private final String name;
    private final String endpoint;

    @JsonCreator
    public EndpointJSON(@JsonProperty("name") String name,
                        @JsonProperty("endpoint") String endpoint) {
        this.name = name;
        this.endpoint = endpoint;
    }

    public EndpointJSON(Document bsonDocument) {
        this.name = bsonDocument.getString("name");
        this.endpoint = bsonDocument.getString("endpoint");
    }

    public String getName() {
        return name;
    }

    public String getEndpoint() {
        return endpoint;
    }
}
