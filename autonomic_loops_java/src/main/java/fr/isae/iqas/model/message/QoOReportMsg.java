package fr.isae.iqas.model.message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 27/02/2017.
 */
public class QoOReportMsg {
    public enum ReportSubject {
        OBS_RATE,
        REPORT,
        KEEP_ALIVE
    }

    private String pipeline_id;

    private String request_id;
    private String producer;
    private Map<String,Object> qooAttributesMap;

    public QoOReportMsg(String pipeline_id) {
        this.pipeline_id = pipeline_id;
        this.producer = "UNKNOWN";
        this.request_id = "UNKNOWN";
        this.qooAttributesMap = new ConcurrentHashMap<>();
    }

    public void setProducerName(String producer) {
        this.producer = producer;
    }

    public String getRequest_id() {
        return request_id;
    }

    public void setRequest_id(String request_id) {
        this.request_id = request_id;
    }

    public String getPipeline_id() {
        return pipeline_id;
    }

    public String getProducer() {
        return producer;
    }

    public void setQooAttribute(String s, Object o) {
        this.qooAttributesMap.put(s, o);
    }

    public Map<String, Object> getQooAttributesMap() {
        return qooAttributesMap;
    }

    public void setQooAttributesMap(Map<String, Object> qooAttributesMap) {
        this.qooAttributesMap = qooAttributesMap;
    }
}
