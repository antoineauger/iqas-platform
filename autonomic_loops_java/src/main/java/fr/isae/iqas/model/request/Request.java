package fr.isae.iqas.model.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;

/**
 * Created by an.auger on 13/09/2016.
 */
public class Request {
    private String request_id;
    private String application_id;
    private ArrayList<State> statesList;

    @JsonCreator
    public Request(@JsonProperty("request_id") String request_id,
                   @JsonProperty("application_id") String application_id) {
        this.request_id = request_id;
        this.application_id = application_id;
        this.statesList = new ArrayList<>();
        this.statesList.add(new State(Status.CREATED, new Date()));
    }

    public Request(Document bsonDocument) {
        request_id = bsonDocument.getString("request_id");
        application_id = bsonDocument.getString("application_id");
    }

    public void setRequest_id(String request_id) {
        this.request_id = request_id;
    }

    public String getRequest_id() {
        return request_id;
    }

    public void setApplication_id(String application_id) {
        this.application_id = application_id;
    }

    public String getApplication_id() {
        return application_id;
    }

    public Document toBSON() {
        Document docToReturn = new Document();
        docToReturn.put("request_id", request_id);
        return docToReturn;
    }

    public boolean isInState(Status status) {
        return statesList.get(statesList.size() - 1).getStatus().equals(status);
    }

    public boolean hasBeenInState(Status status) {
        for (State s : statesList) {
            if (s.getStatus().equals(status)) {
                return true;
            }
        }
        return false;
    }

    public State getStateDetails(Status status) throws Exception {
        for (State s : statesList) {
            if (s.getStatus().equals(status)) {
                return s;
            }
        }
        throw new Exception("iQAS error: Unknown status for this request.");
    }

    public Status getCurrentStatus() {
        return statesList.get(statesList.size() - 1).getStatus();
    }

    public void updateState(Status newStatus) {
        Date currentDate = new Date();
        statesList.get(statesList.size() - 1).setEnd_date(currentDate);
        statesList.add(new State(newStatus, currentDate));
    }

}
