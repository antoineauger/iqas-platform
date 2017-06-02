package fr.isae.iqas.model.message;

import static fr.isae.iqas.model.message.MAPEKenums.ActionMAPEK;
import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK;

/**
 * Created by an.auger on 02/06/2017.
 */
public class ActionMsgKafka extends ActionMsg {
    private final String kafkaTopicID;

    public ActionMsgKafka(ActionMAPEK action, EntityMAPEK about, String kafkaTopicID) {
        super(action, about);
        this.kafkaTopicID = kafkaTopicID;
    }

    public String getKafkaTopicID() {
        return kafkaTopicID;
    }
}
