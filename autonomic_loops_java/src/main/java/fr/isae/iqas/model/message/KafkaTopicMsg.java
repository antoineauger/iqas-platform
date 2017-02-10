package fr.isae.iqas.model.message;

/**
 * Created by an.auger on 13/09/2016.
 */

public class KafkaTopicMsg {
    public enum KafkaSubject {
        CREATE,
        DELETE,
        RESET
    }

    private KafkaSubject kafkaSubject;
    private String topic;

    public KafkaTopicMsg(KafkaSubject kafkaSubject, String topic) {
        this.kafkaSubject = kafkaSubject;
        this.topic = topic;
    }

    public KafkaSubject getKafkaSubject() {
        return kafkaSubject;
    }

    public String getTopic() {
        return topic;
    }
}
