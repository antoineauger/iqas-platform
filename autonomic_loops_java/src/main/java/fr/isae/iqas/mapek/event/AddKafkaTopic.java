package fr.isae.iqas.mapek.event;

/**
 * Created by an.auger on 13/09/2016.
 */
public class AddKafkaTopic {
    private String topic;

    public AddKafkaTopic(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}