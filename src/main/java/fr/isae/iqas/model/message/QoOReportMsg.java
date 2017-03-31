package fr.isae.iqas.model.message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 27/02/2017.
 */
public class QoOReportMsg {
    private String uniquePipelineID;
    private String requestID;
    private String producer;
    private Map<String,String> qooAttributesMap;

    public QoOReportMsg(String uniquePipelineID) {
        this.uniquePipelineID = uniquePipelineID;
        this.producer = "UNKNOWN";
        this.requestID = "UNKNOWN";
        this.qooAttributesMap = new ConcurrentHashMap<>();
    }

    public QoOReportMsg(QoOReportMsg msgToClone) {
        this.uniquePipelineID = msgToClone.getUniquePipelineID();
        this.producer = msgToClone.getProducer();
        this.requestID = msgToClone.getRequestID();
        this.qooAttributesMap = msgToClone.getQooAttributesMap();
    }

    public void setProducerName(String producer) {
        this.producer = producer;
    }

    public String getRequestID() {
        return requestID;
    }

    public void setRequestID(String requestID) {
        this.requestID = requestID;
    }

    public String getUniquePipelineID() {
        return uniquePipelineID;
    }

    public String getProducer() {
        return producer;
    }

    public void setQooAttribute(String k, String v) {
        this.qooAttributesMap.put(k, v);
    }

    public Map<String, String> getQooAttributesMap() {
        return qooAttributesMap;
    }

    public void setQooAttributesMap(Map<String, String> qooAttributesMap) {
        this.qooAttributesMap = qooAttributesMap;
    }
}
