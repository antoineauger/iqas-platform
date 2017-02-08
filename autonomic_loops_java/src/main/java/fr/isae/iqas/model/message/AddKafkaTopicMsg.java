package fr.isae.iqas.model.message;

/**
 * Created by an.auger on 13/09/2016.
 */
public class AddKafkaTopicMsg {
    private String topic;

    public AddKafkaTopicMsg(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}
