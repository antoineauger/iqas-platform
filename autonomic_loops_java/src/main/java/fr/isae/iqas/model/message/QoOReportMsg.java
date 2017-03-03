package fr.isae.iqas.model.message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 27/02/2017.
 */
public class QoOReportMsg {
    public enum ReportSubject {
        KEEP_ALIVE
    }

    private String uniquePipelineID;
    private String requestID;
    private String producer;
    private Map<String,Integer> obsRateByTopic;
    private Map<String,String> qooAttributesMap;

    public QoOReportMsg(String uniquePipelineID) {
        this.uniquePipelineID = uniquePipelineID;
        this.producer = "UNKNOWN";
        this.requestID = "UNKNOWN";
        this.qooAttributesMap = new ConcurrentHashMap<>();
        this.obsRateByTopic = new ConcurrentHashMap<>();
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

    public Map<String, Integer> getObsRateByTopic() {
        return obsRateByTopic;
    }

    public void setObsRateByTopic(Map<String, Integer> obsRateByTopic) {
        this.obsRateByTopic = obsRateByTopic;
    }
}
