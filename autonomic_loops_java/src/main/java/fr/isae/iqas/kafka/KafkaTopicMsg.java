package fr.isae.iqas.kafka;

/**
 * Created by an.auger on 13/09/2016.
 */

public class KafkaTopicMsg {
    public enum TopicAction {
        CREATE,
        DELETE,
        RESET
    }

    private TopicAction topicAction;
    private String topic;

    public KafkaTopicMsg(TopicAction topicAction, String topic) {
        this.topicAction = topicAction;
        this.topic = topic;
    }

    public TopicAction getTopicAction() {
        return topicAction;
    }

    public String getTopic() {
        return topic;
    }
}
