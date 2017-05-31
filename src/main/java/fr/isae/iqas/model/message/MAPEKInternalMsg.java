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
        RESET,
        DELETE,
        TURN_ON,
        TURN_OFF,
        SENSOR_API
    }

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

        // UPDATE for Sensors
        public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about) {
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.symptom = symptom;
            this.about = about;
        }

        // For Requests
        public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about, Request attachedRequest) {
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.symptom = symptom;
            this.about = about;
            this.attachedRequest = attachedRequest;
        }

        // For removed Pipelines (cleanup)
        public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about, String uniqueIDRemovedPipeline) {
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.symptom = symptom;
            this.about = about;
            this.uniqueIDPipeline = uniqueIDRemovedPipeline;
        }

        // For Virtual Sensors connection report
        public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about, Map<String, Boolean> connectedSensors) {
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.symptom = symptom;
            this.about = about;
            this.connectedSensors = connectedSensors;
        }

        // For OBS_RATE TOO_LOW
        public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about, String concernedUniqueIDPipeline, List<String> concernedRequests) {
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.symptom = symptom;
            this.about = about;
            this.uniqueIDPipeline = concernedUniqueIDPipeline;
            this.concernedRequests = concernedRequests;
        }

        // For TOO_HIGH / TOO_LOW QoOAttributes (except OBS_RATE)
        /*public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about, String concernedUniqueIDPipeline, List<String> concernedRequests) {
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.symptom = symptom;
            this.about = about;
            this.uniqueIDPipeline = concernedUniqueIDPipeline;
            this.concernedRequests = concernedRequests;
        }*/

        // For Pipeline creation (Plan -> Monitor)
        public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about, String uniqueIDPipeline, String requestID) {
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
        private RequestMapping oldRequestMapping;
        private RequestMapping newRequestMapping;
        private String associatedRequest_id;

        public RFCMsg(RFCMsg rfcMsgToClone) {
            this.creationDate = rfcMsgToClone.getCreationDate();
            this.rfc = rfcMsgToClone.getRfc();
            this.about = rfcMsgToClone.getAbout();
            this.request = rfcMsgToClone.getRequest();
            this.qoOAttribute = rfcMsgToClone.getQoOAttribute();
            this.newRequestMapping = rfcMsgToClone.getNewRequestMapping();
            this.associatedRequest_id = rfcMsgToClone.getAssociatedRequest_id();
        }

        // HEAL / RESET for QoOAttributes
        public RFCMsg(RFCMAPEK rfc, EntityMAPEK about, HealRequest healRequest, RequestMapping oldRequestMapping, RequestMapping newRequestMapping) {
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.rfc = rfc;
            this.about = about;
            this.qoOAttribute = healRequest.getConcernedAttr();
            this.oldRequestMapping = oldRequestMapping;
            this.newRequestMapping = newRequestMapping;
            this.healRequest = healRequest;
        }

        // UPDATE for Sensors
        public RFCMsg(RFCMAPEK rfc, EntityMAPEK about) {
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.rfc = rfc;
            this.about = about;
        }

        // CREATE for Requests
        public RFCMsg(RFCMAPEK rfc, EntityMAPEK about, Request request, RequestMapping newRequestMapping) {
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.rfc = rfc;
            this.about = about;
            this.request = request;
            this.newRequestMapping = newRequestMapping;
            this.associatedRequest_id = request.getRequest_id();
        }

        // REMOVE for Requests
        public RFCMsg(RFCMAPEK rfc, EntityMAPEK about, Request request) {
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

        public RequestMapping getNewRequestMapping() {
            return newRequestMapping;
        }

        public QoOAttribute getQoOAttribute() {
            return qoOAttribute;
        }

        public HealRequest getHealRequest() {
            return healRequest;
        }

        public RequestMapping getOldRequestMapping() {
            return oldRequestMapping;
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
