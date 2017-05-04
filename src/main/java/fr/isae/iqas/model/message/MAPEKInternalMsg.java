package fr.isae.iqas.model.message;

import fr.isae.iqas.kafka.RequestMapping;
import fr.isae.iqas.model.request.HealRequest;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.quality.QoOAttribute;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.pipelines.IPipeline;

import java.sql.Timestamp;
import java.util.List;
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
        DELETE,
        RESET,
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
     * Symptoms (Emitted by Monitor)
     */

    public static class SymptomMsg {
        private Map<String, Boolean> connectedSensors;
        private List<String> concernedRequests;
        private Timestamp creationDate;
        private SymptomMAPEK symptom;
        private EntityMAPEK about;
        private Request attachedRequest;
        private String uniqueIDPipeline;
        private String requestID;

        public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about) { // UPDATE for Sensors
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.symptom = symptom;
            this.about = about;
        }

        public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about, Request attachedRequest) { // For Requests
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.symptom = symptom;
            this.about = about;
            this.attachedRequest = attachedRequest;
        }

        public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about, String uniqueIDRemovedPipeline) { // For removed Pipelines (cleanup)
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.symptom = symptom;
            this.about = about;
            this.uniqueIDPipeline = uniqueIDRemovedPipeline;
        }

        public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about, Map<String, Boolean> connectedSensors) { // For Virtual Sensors connection report
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.symptom = symptom;
            this.about = about;
            this.connectedSensors = connectedSensors;
        }

        public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about, String concernedUniqueIDPipeline, List<String> concernedRequests) { // For OBS_RATE too low
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.symptom = symptom;
            this.about = about;
            this.uniqueIDPipeline = concernedUniqueIDPipeline;
            this.concernedRequests = concernedRequests;
        }

        public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about, String uniqueIDPipeline, String requestID) { // For Pipeline creation (Plan -> Monitor)
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.symptom = symptom;
            this.about = about;
            this.uniqueIDPipeline = uniqueIDPipeline;
            this.requestID = requestID;
        }

        public Timestamp getCreationDate() {
            return creationDate;
        }

        public SymptomMAPEK getSymptom() {
            return symptom;
        }

        public EntityMAPEK getAbout() {
            return about;
        }

        public Request getAttachedRequest() {
            return attachedRequest;
        }

        public String getUniqueIDPipeline() {
            return uniqueIDPipeline;
        }

        public String getRequestID() {
            return requestID;
        }

        public List<String> getConcernedRequests() {
            return concernedRequests;
        }

        public Map<String, Boolean> getConnectedSensors() {
            return connectedSensors;
        }
    }

    /**
     * Requests for Changes (RFCs) (Analyze -> Plan)
     */

    public static class RFCMsg {
        private Timestamp creationDate;
        private RFCMAPEK rfc;
        private EntityMAPEK about;
        private Request request;
        private HealRequest healRequest;
        private QoOAttribute qoOAttribute;
        private RequestMapping requestMapping;
        private String associatedRequest_id;

        public RFCMsg(RFCMsg rfcMsgToClone) {
            this.creationDate = rfcMsgToClone.getCreationDate();
            this.rfc = rfcMsgToClone.getRfc();
            this.about = rfcMsgToClone.getAbout();
            this.request = rfcMsgToClone.getRequest();
            this.qoOAttribute = rfcMsgToClone.getQoOAttribute();
            this.requestMapping = rfcMsgToClone.getRequestMapping();
            this.associatedRequest_id = rfcMsgToClone.getAssociatedRequest_id();
        }

        public RFCMsg(RFCMAPEK rfc, EntityMAPEK about, HealRequest healRequest) { // HEAL / RESET for QoOAttributes
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.rfc = rfc;
            this.about = about;
            this.qoOAttribute = healRequest.getConcernedAttr();
            this.healRequest = healRequest;
        }

        public RFCMsg(RFCMAPEK rfc, EntityMAPEK about) { // UPDATE for Sensors
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.rfc = rfc;
            this.about = about;
        }

        public RFCMsg(RFCMAPEK rfc, EntityMAPEK about, Request request, RequestMapping requestMapping) { // CREATE for Requests
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.rfc = rfc;
            this.about = about;
            this.request = request;
            this.requestMapping = requestMapping;
            this.associatedRequest_id = request.getRequest_id();
        }

        public RFCMsg(RFCMAPEK rfc, EntityMAPEK about, Request request) { // REMOVE for Requests
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.rfc = rfc;
            this.about = about;
            this.request = request;
            this.associatedRequest_id = request.getRequest_id();
        }

        public RFCMAPEK getRfc() {
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

        public Request getRequest() {
            return request;
        }

        public RequestMapping getRequestMapping() {
            return requestMapping;
        }

        public QoOAttribute getQoOAttribute() {
            return qoOAttribute;
        }
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

        public ActionMsg(ActionMAPEK action, EntityMAPEK about, String kafkaTopicID) { // CREATE / DELETE / RESET KafkaTopic
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.action = action;
            this.about = about;
            this.kafkaTopicID = kafkaTopicID;
        }

        public ActionMsg(ActionMAPEK action,
                         EntityMAPEK about,
                         IPipeline pipelineToEnforce,
                         ObservationLevel askedObsLevel,
                         Set<String> topicsToPullFrom,
                         String topicToPublish,
                         String associatedRequest_id,
                         String constructedFromRequest,
                         int maxLevelDepth) { // APPLY Pipeline
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
