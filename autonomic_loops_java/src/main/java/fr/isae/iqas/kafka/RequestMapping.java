package fr.isae.iqas.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import fr.isae.iqas.model.request.Request;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 28/02/2017.
 */
public class RequestMapping {

    private String request_id;
    private String constructedFromRequest;
    private Set<String> dependentRequests;
    private Map<String, TopicEntity> allTopics;

    @JsonCreator
    public RequestMapping(String request_id) {
        this.request_id = request_id;
        this.constructedFromRequest = "";
        this.dependentRequests = new HashSet<>();
        this.allTopics = new ConcurrentHashMap<>();
    }

    public RequestMapping(Document bsonDocument) {
        this.request_id = bsonDocument.getString("request_id");
        this.constructedFromRequest = bsonDocument.getString("constructed_from");

        this.dependentRequests = new HashSet<>();
        List<String> reqIDsList = (List<String>) bsonDocument.get("dependent_requests");
        for (String s : reqIDsList) {
            dependentRequests.add(s);
        }

        this.allTopics = new ConcurrentHashMap<>();
        Map<String, Document> topicsDoc = (Map<String, Document>) bsonDocument.get("all_topics");
        topicsDoc.forEach((key, value) -> {
            allTopics.put(key, new TopicEntity(value));
        });
    }

    public @JsonIgnore Document toBSON() {
        Document docToReturn = new Document();

        docToReturn.put("constructed_from", constructedFromRequest);
        docToReturn.put("request_id", request_id);

        List<String> dependentRequestsText = new ArrayList<>();
        for (String s : dependentRequests) {
            dependentRequestsText.add(s);
        }
        docToReturn.put("dependent_requests", dependentRequestsText);

        Map<String, Document> bsonTopicEntityMap = new ConcurrentHashMap<>();
        allTopics.forEach((key, value) -> bsonTopicEntityMap.put(key, value.toBSON()));
        docToReturn.put("all_topics", bsonTopicEntityMap);

        return docToReturn;
    }

    public void addLink(String topic1, String topic2, String pipeline_id) {
        if (allTopics.get(topic1) != null && allTopics.get(topic2) != null) {
            TopicEntity t1 = allTopics.get(topic1);
            TopicEntity t2 = allTopics.get(topic2);

            t2.getParents().add(t1.getName());
            t1.getChildren().add(t2.getName());
            t1.getEnforcedPipelines().put(t2.getName(), pipeline_id);
        }
    }

    public @JsonIgnore List<TopicEntity> getPrimarySources() {
        List<TopicEntity> topicListToReturn = new ArrayList<>();
        allTopics.forEach((key, value) -> {
            if (value.isSource()) {
                topicListToReturn.add(value);
            }
        });
        return topicListToReturn;
    }

    public @JsonIgnore TopicEntity getFinalSink() {
        for (Object o : allTopics.entrySet()) {
            Map.Entry pair = (Map.Entry) o;
            TopicEntity topicEntityTemp = (TopicEntity) pair.getValue();

            if (topicEntityTemp.isSink()) {
                return topicEntityTemp;
            }
        }
        return null;
    }

    public void addDependentRequest(Request r) {
        dependentRequests.add(r.getRequest_id());
    }

    public void setConstructedFromRequest(String request_id) {
        this.constructedFromRequest = request_id;
    }

    @JsonProperty("request_id")
    public String getRequest_id() {
        return request_id;
    }

    @JsonProperty("all_topics")
    public Map<String, TopicEntity> getAllTopics() {
        return allTopics;
    }

    @JsonProperty("dependent_requests")
    public Set<String> getDependentRequests() {
        return dependentRequests;
    }

    @JsonProperty("constructed_from")
    public String getConstructedFromRequest() {
        return constructedFromRequest;
    }
}
