package fr.isae.iqas.model.message;

import fr.isae.iqas.kafka.RequestMapping;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.quality.QoOAttribute;
import fr.isae.iqas.model.request.HealRequest;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.pipelines.IPipeline;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by an.auger on 21/02/2017.
 */
public class MAPEKenums {
    public enum SymptomMAPEK {
        NEW,
        UPDATED,
        REMOVED,
        TOO_HIGH,
        TOO_LOW,
        CONNECTION_REPORT
    }

    public enum EntityMAPEK {
        REQUEST,
        PIPELINE,
        OBS_RATE,
        SENSOR,
        KAFKA_TOPIC
    }

    public enum RFCMAPEK {
        CREATE,
        UPDATE,
        REMOVE,
        HEAL,
        RESET
    }

    public enum ActionMAPEK {
        APPLY,
        CREATE,
        RESET,
        DELETE,
        TURN_ON,
        TURN_OFF,
        SENSOR_API
    }

    /**
     * Actions (Performed by Plan actor)
     */

    public static class ActionMsg {
        private Timestamp creationDate;
        private ActionMAPEK action;
        private EntityMAPEK about;
        private Set<String> topicsToPullFrom;
        private String topicToPublish;
        private IPipeline pipelineToEnforce;
        private ObservationLevel askedObsLevel;
        private String associatedRequest_id;
        private String kafkaTopicID;
        private String constructedFromRequest;
        private int maxLevelDepth;

        // CREATE / DELETE / RESET KafkaTopic
        public ActionMsg(ActionMAPEK action, EntityMAPEK about, String kafkaTopicID) {
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.action = action;
            this.about = about;
            this.kafkaTopicID = kafkaTopicID;
        }

        // APPLY Pipeline
        public ActionMsg(ActionMAPEK action,
                         EntityMAPEK about,
                         IPipeline pipelineToEnforce,
                         ObservationLevel askedObsLevel,
                         Set<String> topicsToPullFrom,
                         String topicToPublish,
                         String associatedRequest_id,
                         String constructedFromRequest,
                         int maxLevelDepth) {
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.action = action;
            this.about = about;
            this.askedObsLevel = askedObsLevel;
            this.pipelineToEnforce = pipelineToEnforce;
            this.topicsToPullFrom = topicsToPullFrom;
            this.topicToPublish = topicToPublish;
            this.associatedRequest_id = associatedRequest_id;
            this.constructedFromRequest = constructedFromRequest;
            this.maxLevelDepth = maxLevelDepth;
        }

        public Timestamp getCreationDate() {
            return creationDate;
        }

        public EntityMAPEK getAbout() {
            return about;
        }

        public String getAssociatedRequest_id() {
            return associatedRequest_id;
        }

        public ActionMAPEK getAction() {
            return action;
        }

        public String getKafkaTopicID() {
            return kafkaTopicID;
        }

        public IPipeline getPipelineToEnforce() {
            return pipelineToEnforce;
        }

        public Set<String> getTopicsToPullFrom() {
            return topicsToPullFrom;
        }

        public String getTopicToPublish() {
            return topicToPublish;
        }

        public String getConstructedFromRequest() {
            return constructedFromRequest;
        }

        public int getMaxLevelDepth() {
            return maxLevelDepth;
        }

        public ObservationLevel getAskedObsLevel() {
            return askedObsLevel;
        }

        public void setPipelineToEnforce(IPipeline pipelineToEnforce) {
            this.pipelineToEnforce = pipelineToEnforce;
        }
    }

}
