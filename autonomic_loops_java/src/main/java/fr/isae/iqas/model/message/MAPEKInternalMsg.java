package fr.isae.iqas.model.message;

import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;

/**
 * Created by an.auger on 21/02/2017.
 */
public class MAPEKInternalMsg {
    public enum SymptomMAPEK {
        NEW,
        UPDATED,
        REMOVED,
        TOO_HIGH,
        TOO_LOW
    }

    public enum EntityMAPEK {
        REQUEST,
        PIPELINE,
        QOO_ATTRIBUTE,
        OBS_RATE,
        SENSOR
    }

    public enum RFCMAPEK {
        INCREASE,
        DECREASE,
        CREATE,
        UPDATE,
        REMOVE
    }

    public enum ActionMAPEK {
        APPLY,
        CREATE,
        DELETE,
        RESET,
        UPDATE,
        TURN_ON,
        TURN_OFF,
        SENSOR_API
    }

    /**
     * QoOAttribute TOO_LOW / TOO_HIGH for request_id
     * OBS_RATE TOO_LOW / TOO_HIGH for request_id
     *
     * NEW request with request_id
     * UPDATED request with request_id
     * REMOVED request with request_id
     */

    /**
     * INCREASE QoOAttribute for request_id
     * DECREASE QoOAttribute for request_id
     *
     * INCREASE OBS_RATE for sensor_id
     * DECREASE OBS_RATE for sensor_id
     */

    /**
     * APPLY PIPELINE for request_id
     * DELETE PIPELINE for request_id
     * UPDATE PIPELINE pipeline_id, new_customizable_params
     *
     * SET OBS_RATE for sensor_id with new_value
     *
     * TURN ON / TURN OFF sensor_id
     *
     * SEND instructions to sensor_id via API
     *      MODIFY OBS_RATE for sensor_id
     */

    /**
     * Internal actions
     *
     * CREATE KAFKA_TOPIC with topic_id
     * DELETE KAFKA_TOPIC with topic_id
     * RESET KAFKA_TOPIC with topic_id
     */


    /**
     * Symptoms (Monitor -> Analyze)
     */

    public static class SymptomMsg {
        private Timestamp creationDate;
        private SymptomMAPEK symptom;
        private EntityMAPEK about;
        private Object attachedObject;

        public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about, Object attachedObject) { // Only 1 constructor
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.symptom = symptom;
            this.about = about;
            this.attachedObject = attachedObject;
        }

        public Timestamp getCreationDate() {
            return creationDate;
        }

        public SymptomMAPEK getMsgType() {
            return symptom;
        }

        public EntityMAPEK getAbout() {
            return about;
        }

        public Object getAttachedObject() {
            return attachedObject;
        }
    }

    /**
     * Requests for Changes (RFCs) (Analyze -> Plan)
     */

    public static class RFCMsg {
        private Timestamp creationDate;
        private RFCMAPEK rfc;
        private EntityMAPEK about;
        private Object attachedObject;
        private String associatedRequest_id;

        public RFCMsg(RFCMAPEK rfc, EntityMAPEK about, Object attachedObject, String associatedRequest_id) { // INCREASE / DECREASE for QoOAttributes
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.rfc = rfc;
            this.about = about;
            this.attachedObject = attachedObject;
            this.associatedRequest_id = associatedRequest_id;
        }

        public RFCMsg(RFCMAPEK rfc, EntityMAPEK about, Object attachedObject) { // OBS_RATE for sensors + CREATE / UPDATE / REMOVE for Sensors, Pipelines, Requests
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.rfc = rfc;
            this.about = about;
            this.attachedObject = attachedObject;
            this.associatedRequest_id = "UNKNOWN";
        }

        public RFCMAPEK getRemedyToPlan() {
            return rfc;
        }

        public String getAssociatedRequest_id() {
            return associatedRequest_id;
        }

        public Timestamp getCreationDate() {
            return creationDate;
        }

        public EntityMAPEK getAbout() {
            return about;
        }

        public Object getAttachedObject() {
            return attachedObject;
        }
    }

    /**
     * Actions (Received by Plan actor)
     */

    public static class ActionMsg {
        private Timestamp creationDate;
        private ActionMAPEK action;
        private EntityMAPEK about;
        private Object attachedObject1;
        private Object attachedObject2;
        private Object attachedObject3;
        private String associatedRequest_id;

        public ActionMsg(ActionMAPEK action, EntityMAPEK about, String kafkaTopicID) { // CREATE / DELETE / RESET KafkaTopic
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.action = action;
            this.about = about;
            this.attachedObject1 = kafkaTopicID;
            this.attachedObject2 = null;
            this.attachedObject3 = null;
            this.associatedRequest_id = "UNKNOWN";
        }

        public ActionMsg(ActionMAPEK action, EntityMAPEK about, String pipeline_id, Set<String> topicsToPullFrom, String topicToPublish, String associatedRequest_id) { // APPLY Pipeline
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.action = action;
            this.about = about;
            this.attachedObject1 = pipeline_id;
            this.attachedObject2 = topicsToPullFrom;
            this.attachedObject3 = topicToPublish;
            this.associatedRequest_id = associatedRequest_id;
        }

        public ActionMsg(ActionMAPEK action, EntityMAPEK about, String pipeline_id, boolean force) { // DELETE Pipeline
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.action = action;
            this.about = about;
            this.attachedObject1 = pipeline_id;
            this.attachedObject2 = null;
            this.attachedObject3 = null;
            this.associatedRequest_id = pipeline_id.split("_")[1];
        }

        public ActionMsg(ActionMAPEK action, EntityMAPEK about, String pipeline_id, Map<String, String> paramsToUpdate) { // UPDATE Pipeline
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.action = action;
            this.about = about;
            this.attachedObject1 = pipeline_id;
            this.attachedObject2 = paramsToUpdate;
            this.attachedObject3 = null;
            this.associatedRequest_id = pipeline_id.split("_")[1];
        }

        public Timestamp getCreationDate() {
            return creationDate;
        }

        public EntityMAPEK getAbout() {
            return about;
        }

        public Object getAttachedObject1() {
            return attachedObject1;
        }

        public Object getAttachedObject2() {
            return attachedObject2;
        }

        public String getAssociatedRequest_id() {
            return associatedRequest_id;
        }

        public ActionMAPEK getAction() {
            return action;
        }

        public Object getAttachedObject3() {
            return attachedObject3;
        }
    }

}
