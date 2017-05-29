package fr.isae.iqas.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import fr.isae.iqas.model.jsonld.QoOPipeline;
import fr.isae.iqas.model.request.Request;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static fr.isae.iqas.model.observation.ObservationLevel.RAW_DATA;

/**
 * Created by an.auger on 28/02/2017.
 */
public class RequestMapping {
    private String application_id;
    private String request_id;
    private String constructedFromRequest;
    private Set<String> dependentRequests;
    private Map<String, TopicEntity> allTopics;

    @JsonCreator
    public RequestMapping(String application_id, String request_id) {
        this.application_id = application_id;
        this.request_id = request_id;
        this.constructedFromRequest = "";
        this.dependentRequests = new HashSet<>();
        this.allTopics = new ConcurrentHashMap<>();
    }

    public  @JsonIgnore RequestMapping(RequestMapping requestMappingToClone) {
        this.application_id = requestMappingToClone.application_id;
        this.request_id = requestMappingToClone.request_id;
        this.constructedFromRequest = requestMappingToClone.constructedFromRequest;
        this.dependentRequests = new HashSet<>(requestMappingToClone.dependentRequests);
        this.allTopics = new ConcurrentHashMap<>(requestMappingToClone.allTopics);
    }

    public RequestMapping(Document bsonDocument) {
        this.request_id = bsonDocument.getString("request_id");
        this.application_id = bsonDocument.getString("application_id");
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
        docToReturn.put("application_id", application_id);
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

            if (!t1.isSource() && t1.getParents().size() > 0) {
                t1.setLevel(allTopics.get(t1.getParents().get(0)).getLevel() + 1);
            }
            t2.setLevel(t1.getLevel() + 1);

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

    public @JsonIgnore List<String> removeAllTopicsAfter(String topicStartingPoint) {
        List<String> removedTopics = new ArrayList<>();
        TopicEntity currTopicEntity = allTopics.get(topicStartingPoint);
        while (!currTopicEntity.isSink()) {
            for (String child : currTopicEntity.getChildren()) {
                allTopics.remove(child);
                removedTopics.add(child);
            }
        }
        removedTopics.add(currTopicEntity.getName());
        allTopics.remove(currTopicEntity.getName());
        return removedTopics;
    }

    public void addDependentRequest(Request r) {
        dependentRequests.add(r.getRequest_id());
    }

    public void setConstructedFromRequest(String request_id) {
        this.constructedFromRequest = request_id;
    }

    @JsonProperty("application_id")
    public String getApplication_id() {
        return application_id;
    }

    public void setApplication_id(String application_id) {
        this.application_id = application_id;
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

    public void healRequestWith(QoOPipeline healPipeline) {
        TopicEntity newHealTopic = new TopicEntity(application_id + "_" + request_id + "_" + healPipeline.pipeline, getFinalSink().getObservationLevel());
        newHealTopic.setForHeal(true);

        TopicEntity finalSink = getFinalSink();
        TopicEntity oldJustBeforeLastTopic = allTopics.get(getFinalSink().getParents().get(0));

        finalSink.getParents().clear();
        oldJustBeforeLastTopic.getChildren().clear();

        oldJustBeforeLastTopic.getChildren().add(newHealTopic.getName());
        newHealTopic.getParents().add(oldJustBeforeLastTopic.getName());
        newHealTopic.getChildren().add(finalSink.getName());
        newHealTopic.getEnforcedPipelines().put(finalSink.getName(), healPipeline.pipeline);
        finalSink.getParents().add(newHealTopic.getName());

        newHealTopic.setLevel(oldJustBeforeLastTopic.getLevel() + 1);
        finalSink.setLevel(newHealTopic.getLevel() + 1);

        allTopics.put(oldJustBeforeLastTopic.getName(), oldJustBeforeLastTopic);
        allTopics.put(newHealTopic.getName(), newHealTopic);
        allTopics.put(finalSink.getName(), finalSink);
    }

    public void resetRequestHeal() {
        TopicEntity currTopic = getPrimarySources().get(0);
        TopicEntity lastBeforeHealTopic = currTopic;
        TopicEntity finalSinkTopic = getFinalSink();

        List<String> topicEntityToDelete = new ArrayList<>();
        while (currTopic.getChildren().size() > 0) {
            if (currTopic.isForHeal()) {
                topicEntityToDelete.add(currTopic.getName());
            }
            else {
                lastBeforeHealTopic = currTopic;
            }
            currTopic = allTopics.get(currTopic.getChildren().get(0));
        }

        lastBeforeHealTopic.getChildren().clear();
        finalSinkTopic.getParents().clear();

        finalSinkTopic.getParents().add(lastBeforeHealTopic.getName());
        lastBeforeHealTopic.getChildren().add(finalSinkTopic.getName());
        finalSinkTopic.setLevel(lastBeforeHealTopic.getLevel() + 1);
        allTopics.put(lastBeforeHealTopic.getName(), lastBeforeHealTopic);
        allTopics.put(finalSinkTopic.getName(), finalSinkTopic);

        for (String t : topicEntityToDelete) {
            allTopics.remove(t);
        }
    }
}
