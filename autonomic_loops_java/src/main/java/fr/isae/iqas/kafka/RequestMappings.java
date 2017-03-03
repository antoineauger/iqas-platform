package fr.isae.iqas.kafka;

import fr.isae.iqas.model.request.Request;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 28/02/2017.
 */
public class RequestMappings {
    private Set<Request> associatedRequests;
    private Map<String, TopicEntity> allTopics;

    public RequestMappings() {
        this.associatedRequests = new HashSet<>();
        this.allTopics = new ConcurrentHashMap<>();
    }

    public void addLink(String topic1, String topic2, String pipeline_id) {
        if (allTopics.get(topic1) != null && allTopics.get(topic2) != null) {
            TopicEntity t1 = allTopics.get(topic1);
            TopicEntity t2 = allTopics.get(topic2);

            t2.getParents().add(t1);
            t1.getChildren().add(t2);
            t1.getEnforcedPipelines().put(t2, pipeline_id);
        }
    }

    public List<TopicEntity> getPrimarySources() {
        List<TopicEntity> topicListToReturn = new ArrayList<>();

        for (Object o : allTopics.entrySet()) {
            Map.Entry pair = (Map.Entry) o;
            TopicEntity topicEntityTemp = (TopicEntity) pair.getValue();

            if (topicEntityTemp.isSource()) {
                topicListToReturn.add(topicEntityTemp);
            }
        }

        return topicListToReturn;
    }

    public TopicEntity getFinalSink() {
        for (Object o : allTopics.entrySet()) {
            Map.Entry pair = (Map.Entry) o;
            TopicEntity topicEntityTemp = (TopicEntity) pair.getValue();

            if (topicEntityTemp.isSink()) {
                return topicEntityTemp;
            }
        }
        return null;
    }

    public void addAssociatedRequest(Request r) {
        associatedRequests.add(r);
    }

    public Set<Request> getAssociatedRequests() {
        return associatedRequests;
    }

    public Map<String, TopicEntity> getAllTopics() {
        return allTopics;
    }
}
