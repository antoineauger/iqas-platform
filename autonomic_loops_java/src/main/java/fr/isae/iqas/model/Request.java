package fr.isae.iqas.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.Document;

/**
 * Created by an.auger on 13/09/2016.
 */
public class Request {
    private String request_id;

    @JsonCreator
    public Request(@JsonProperty("request_id") String request_id) {
        this.request_id = request_id;
    }

    public Request(Document bsonDocument) {
        this.request_id = bsonDocument.getString("request_id");
    }

    public void setRequest_id(String request_id) {
        this.request_id = request_id;
    }

    public String getRequest_id() {
        return request_id;
    }

    public Document toBSON() {
        Document docToReturn = new Document();
        docToReturn.put("request_id", request_id);
        return docToReturn;
    }
}
