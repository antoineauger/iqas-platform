package fr.isae.iqas.kafka;

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
    private int level;
    private String name;
    private String forTopic;
    private String forApplication;
    private ObservationLevel observationLevel;
    private boolean isSource;
    private boolean isForHeal;
    private boolean isSink;

    private List<String> parents;
    private List<String> children;
    private Map<String, String> enforcedPipelines; // EnforcedPipelines with uniqueID

    public TopicEntity(TopicEntity topicEntityToClone) { // To clone TopicEntity object
        this.level = topicEntityToClone.level;
        this.name = topicEntityToClone.name;
        this.isSource = topicEntityToClone.isSource;
        this.isSink = topicEntityToClone.isSink;
        this.isForHeal = topicEntityToClone.isForHeal;
        this.observationLevel = topicEntityToClone.observationLevel;

        this.parents = topicEntityToClone.parents;
        this.children = topicEntityToClone.parents;
        this.enforcedPipelines = topicEntityToClone.enforcedPipelines;

        this.forTopic = topicEntityToClone.forTopic;
        this.forApplication = topicEntityToClone.forApplication;
    }

    public TopicEntity(String name, ObservationLevel observation_level) {
        this.level = 0;
        this.name = name;
        this.isSource = false;
        this.isForHeal = false;
        this.isSink = false;
        this.observationLevel = observation_level;

        this.parents = new ArrayList<>();
        this.children = new ArrayList<>();
        this.enforcedPipelines = new ConcurrentHashMap<>();

        this.forTopic = "";
        this.forApplication = "";
    }

    public Document toBSON() {
        Document docToReturn = new Document();
        docToReturn.put("name", name);
        docToReturn.put("level", level);
        docToReturn.put("isSource", isSource);
        docToReturn.put("isForHeal", isForHeal);
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
        this.level = bsonDocument.getInteger("level");
        this.name = bsonDocument.getString("name");
        this.isSource = bsonDocument.getBoolean("isSource");
        this.isForHeal = bsonDocument.getBoolean("isForHeal");
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

    public ObservationLevel getObservationLevel() {
        return observationLevel;
    }

    public String getObservationLevelString() {
        return observationLevel.toString();
    }

    public List<String> getParents() {
        return parents;
    }

    public List<String> getChildren() {
        return children;
    }

    public Map<String, String> getEnforcedPipelines() {
        return enforcedPipelines;
    }

    public String getForTopic() {
        return forTopic;
    }

    public String getForApplication() {
        return forApplication;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public void setObservationLevel(ObservationLevel observationLevel) {
        this.observationLevel = observationLevel;
    }


    public boolean isForHeal() {
        return isForHeal;
    }

    public void setForHeal(boolean forHeal) {
        isForHeal = forHeal;
    }
}
