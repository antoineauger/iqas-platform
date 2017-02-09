package fr.isae.iqas.model.message;

/**
 * Created by an.auger on 13/09/2016.
 */

public class KafkaTopicMsg {
    public enum Subject {
        CREATE,
        DELETE,
        RESET
    }

    private Subject subject;
    private String topic;

    public KafkaTopicMsg(Subject subject, String topic) {
        this.subject = subject;
        this.topic = topic;
    }

    public Subject getSubject() {
        return subject;
    }

    public String getTopic() {
        return topic;
    }
}
