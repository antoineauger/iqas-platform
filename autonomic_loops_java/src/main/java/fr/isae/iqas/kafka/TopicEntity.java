package fr.isae.iqas.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import fr.isae.iqas.model.observation.ObservationLevel;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 28/02/2017.
 */
public class TopicEntity {
    private String name;
    private String forTopic;
    private String forApplication;
    private ObservationLevel observationLevel;
    private boolean isSource;
    private boolean isSink;

    private List<String> parents;
    private List<String> children;
    private Map<String, String> enforcedPipelines; // EnforcedPipelines with uniqueID

    @JsonCreator
    public TopicEntity(@JsonProperty("name") String name) {
        this.name = name;
        this.isSource = false;
        this.isSink = false;
        this.observationLevel = ObservationLevel.INFORMATION; //TODO make this dynamic

        this.parents = new ArrayList<>();
        this.children = new ArrayList<>();
        this.enforcedPipelines = new ConcurrentHashMap<>();

        this.forTopic = "";
        this.forApplication = "";
    }

    public @JsonIgnore Document toBSON() {
        Document docToReturn = new Document();
        docToReturn.put("name", name);
        docToReturn.put("isSource", isSource);
        docToReturn.put("isSink", isSink);
        docToReturn.put("observation_level", observationLevel.toString());
        docToReturn.put("parents", parents);
        docToReturn.put("children", children);
        docToReturn.put("enforced_pipelines", enforcedPipelines);
        docToReturn.put("for_topic", forTopic);
        docToReturn.put("for_application", forApplication);
        return docToReturn;
    }

    public TopicEntity(Document bsonDocument) {
        this.name = bsonDocument.getString("name");
        this.isSource = bsonDocument.getBoolean("isSource");
        this.isSink = bsonDocument.getBoolean("isSink");
        this.observationLevel = ObservationLevel.valueOf(bsonDocument.getString("observation_level"));

        this.parents = new ArrayList<>();
        List<String> parentsList = (List<String>) bsonDocument.get("parents");
        this.parents.addAll(parentsList);

        this.children = new ArrayList<>();
        List<String> childrenList = (List<String>) bsonDocument.get("children");
        this.children.addAll(childrenList);

        this.enforcedPipelines = new ConcurrentHashMap<>();
        Map<String, String> enforcedPipelinesMap = (Map<String, String>) bsonDocument.get("enforced_pipelines");
        this.enforcedPipelines.putAll(enforcedPipelinesMap);

        this.forTopic = bsonDocument.getString("for_topic");
        this.forApplication = bsonDocument.getString("for_application");
    }

    public void setSource(String forTopic) {
        this.isSource = true;
        this.forTopic = forTopic;
    }

    public void setSink(String forApplication) {
        this.isSink = true;
        this.forApplication = forApplication;
    }

    public String getName() {
        return name;
    }

    public boolean isSource() {
        return isSource;
    }

    public boolean isSink() {
        return isSink;
    }

    public @JsonIgnore ObservationLevel getObservationLevel() {
        return observationLevel;
    }

    @JsonProperty("observation_level")
    public String getObservationLevelString() {
        return observationLevel.toString();
    }

    public List<String> getParents() {
        return parents;
    }

    public List<String> getChildren() {
        return children;
    }

    @JsonProperty("enforced_pipelines")
    public Map<String, String> getEnforcedPipelines() {
        return enforcedPipelines;
    }

    @JsonProperty("for_topic")
    public String getForTopic() {
        return forTopic;
    }

    @JsonProperty("for_application")
    public String getForApplication() {
        return forApplication;
    }
}
