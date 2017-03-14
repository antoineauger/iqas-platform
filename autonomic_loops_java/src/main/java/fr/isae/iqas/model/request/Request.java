package fr.isae.iqas.model.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import fr.isae.iqas.model.quality.QoOAttribute;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static fr.isae.iqas.model.request.State.Status.CREATED;

/**
 * Created by an.auger on 13/09/2016.
 */

public class Request {

    private String request_id;
    private String application_id;
    private String topic;
    private String location;
    private QoORequirements qooConstraints;
    private @JsonIgnore ArrayList<State> statesList;

    private ArrayList<String> logs;

    @JsonCreator
    public Request(@JsonProperty("request_id") String request_id,
                   @JsonProperty("application_id") String application_id,
                   @JsonProperty("topic") String topic,
                   @JsonProperty("location") String location,
                   @JsonProperty("qoo") QoORequirements qooConstraints) {
        this.request_id = request_id;
        this.application_id = application_id;
        this.topic = topic;
        this.location = location;

        this.qooConstraints = qooConstraints;

        this.statesList = new ArrayList<>();
        this.statesList.add(new State(CREATED, new Date()));
        this.logs = new ArrayList<>();
        addLog("Object request has been created.");
    }

    public void setRequest_id(String request_id) {
        this.request_id = request_id;
    }

    public void setApplication_id(String application_id) {
        this.application_id = application_id;
    }

    public String getRequest_id() {
        return request_id;
    }

    public String getApplication_id() {
        return application_id;
    }

    public ArrayList<State> getStatesList() {
        return statesList;
    }

    /**
     * 2nd constructor used to construct a Request object from a BSON document
     *
     * @param bsonDocument the BSON document that will be used to construct the Request object
     */
    public Request(Document bsonDocument) {
        this.request_id = bsonDocument.getString("request_id");
        this.application_id = bsonDocument.getString("application_id");
        this.topic = bsonDocument.getString("topic");
        this.location = bsonDocument.getString("location");

        Document qooDoc = (Document) bsonDocument.get("qoo");
        this.qooConstraints = new QoORequirements(
                qooDoc.getString("operator"),
                qooDoc.getString("sla_level"),
                (List<String>) qooDoc.get("interested_in"),
                (Map<String, String>) qooDoc.get("additional_params"));

        this.statesList = new ArrayList<>();
        List<Document> bsonStatesList = (List<Document>) bsonDocument.get("statesList");
        this.statesList.addAll(bsonStatesList.stream().map(d ->
                new State(State.Status.valueOf(d.getString("status")),
                new Date(d.getLong("start_date")),
                new Date(d.getLong("end_date")))).collect(Collectors.toList()));

        this.logs = new ArrayList<>();
        List<String> logList = (List<String>) bsonDocument.get("logs");
        this.logs.addAll(logList);
    }

    /**
     * 3rd constructor useful to ask Request deletion
     *
     * @param request_id the id of the request to delete
     */
    public Request(String request_id) {
        this.request_id = request_id;
        this.application_id = "";
        this.statesList = new ArrayList<>();
    }

    /**
     * Method called to transform a Request object into a BSON object
     *
     * @return a BSON object ready to be inserted into MongoDB database
     */
    public @JsonIgnore Document toBSON() {
        Document docToReturn = new Document();
        docToReturn.put("request_id", request_id);
        docToReturn.put("application_id", application_id);
        docToReturn.put("topic", topic);
        docToReturn.put("location", location);
        docToReturn.put("logs", logs);

        Document qooDoc = new Document();

        qooDoc.put("sla_level", qooConstraints.getSla_level().toString());
        qooDoc.put("operator", qooConstraints.getOperator().toString());
        qooDoc.put("additional_params", qooConstraints.getAdditional_params());
        List<String> interestedInText = new ArrayList<>();
        for (QoOAttribute q : qooConstraints.getInterested_in()) {
            interestedInText.add(q.toString());
        }
        qooDoc.put("interested_in", interestedInText);

        docToReturn.put("qoo", qooDoc);

        List<Document> statesListDoc = new ArrayList<>();
        for (State s : statesList) {
            Document tempStateDoc = new Document();
            tempStateDoc.put("status", s.getStatus().toString());
            tempStateDoc.put("start_date", s.getStart_date().getTime());
            tempStateDoc.put("end_date", s.getEnd_date().getTime());
            statesListDoc.add(tempStateDoc);
        }
        docToReturn.put("statesList", statesListDoc);

        return docToReturn;
    }

    /**
     * Method to know if a Request has been in a specific state (current or past)
     *
     * @param status the given Status to look for in the state list
     * @return a boolean (true/false)
     */
    public @JsonIgnore boolean hasBeenInState(State.Status status) {
        for (State s : statesList) {
            if (s.getStatus().equals(status)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Method to get details (start and end dates) about a specific state (current or past)
     * of a Request
     *
     * @param status the given Status to look for in the state list
     * @return a State object corresponding to the given Status for the Request
     * @throws Exception if the Request has not been in the specified Status
     */
    public @JsonIgnore State getStateDetails(State.Status status) throws Exception {
        for (State s : statesList) {
            if (s.getStatus().equals(status)) {
                return s;
            }
        }
        throw new Exception("iQAS error: No state with this status for this request.");
    }

    /**
     * Method to get the current Status of a Request
     *
     * @return the current Status object
     */
    public State.Status getCurrent_status() {
        if (statesList.size() > 0) {
            return statesList.get(statesList.size() - 1).getStatus();
        }
        else {
            return statesList.get(0).getStatus();
        }
    }

    /**
     * Method to compare the current Status with a specified one
     *
     * @param status a Status object
     * @return a boolean (true/false) according to the value of the current Status
     */
    public @JsonIgnore boolean isInState(State.Status status) {
        return getCurrent_status().equals(status);
    }

    /**
     * Method to update the current State of a Request
     * The end date of the previous State is set to Now()
     * The start date of the new State is set to Now()
     *
     * @param newStatus the new Status enum object
     */
    public void updateState(State.Status newStatus) {
        Date currentDate = new Date();
        if (statesList.size() > 0) {
            statesList.get(statesList.size() - 1).setEnd_date(currentDate);
        }
        statesList.add(new State(newStatus, currentDate));
    }

    public void addLog(String s) {
        Date date = new Date();
        logs.add(0, date.toString() + ": " + s);
    }

    public ArrayList<String> getLogs() {
        return logs;
    }

    @JsonProperty("qoo")
    public QoORequirements getQooConstraints() {
        return qooConstraints;
    }

    public String getTopic() {
        return topic;
    }

    public String getLocation() {
        return location;
    }

    @Override
    public boolean equals(Object other){
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof Request)) return false;
        Request otherMyClass = (Request) other;

        // Two requests are considered the same if they have the same topic / location / qooConstraints
        return (otherMyClass.getLocation().equals(this.location)
                && otherMyClass.getTopic().equals(this.topic)
                && otherMyClass.getQooConstraints().equals(this.qooConstraints));
    }
}
