package fr.isae.iqas.model.message;

import java.sql.Timestamp;

/**
 * Created by an.auger on 21/02/2017.
 */
public class MAPEKInternalMsg {
    public enum MAPEKType {
        NEW,
        MODIFIED,
        REMOVED
    }

    public enum MAPEKEntity {
        REQUEST,
        PIPELINE,
        QOO_REPORT,
        SENSOR
    }

    /**
     * QoOAttribute TOO_LOW / TOO_HIGH for request_id
     * OBS_RATE TOO_LOW / TOO_HIGH for request_id
     */

    /**
     * INCREASE QoOAttribute for request_id
     * DECREASE QoOAttribute for request_id
     *
     * INCREASE OBS_RATE for sensor_id
     * DECREASE OBS_RATE for sensor_id
     */

    /**
     * DELETE PIPELINE for request_id
     * APPLY PIPELINE for request_id
     * UPDATE PIPELINE for request_id WITH new_pipeline_id, new_customizable_params
     *
     * MODIFY OBS_RATE for sensor_id
     * TURN ON / TURN OFF sensor_id
     */


    /**
     * CREATE KAFKA TOPIC
     * DELETE KAFKA TOPIC
     * RESET KAFKA TOPIC
     *
     * APPLY pipeline between topic1 and topic2
     * REMOVE pipeline between topic1 and topic2
     *
     * SEND instructions to sensor_id via api
     */


    /**
     * Symptoms (Monitor -> Analyze)
     */

    public static class SymptomMsg {
        private MAPEKType msgType;
        private MAPEKEntity symptom;
        private Timestamp creationDate;

        public SymptomMsg(MAPEKType msgType, MAPEKEntity symptom) {
            this.creationDate = new Timestamp(System.currentTimeMillis());
            this.msgType = msgType;
            this.symptom = symptom;
        }
    }

    /**
     * Request for changes (RFCs) (Analyze -> Plan)
     */

    public static class RFCMsg {
        private String remedyToPlan;
        private String associatedRequest_id;

        public RFCMsg(String remedyToPlan, String associatedRequest_id) {
            this.remedyToPlan = remedyToPlan;
            this.associatedRequest_id = associatedRequest_id;
        }

        public String getRemedyToPlan() {
            return remedyToPlan;
        }

        public String getAssociatedRequest_id() {
            return associatedRequest_id;
        }
    }

    /**
     * Orders (Plan -> Execute)
     */

    public class OrderMsg {
        public OrderMsg() {
        }
    }
    
}
