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

    private String producer;
    private Map<String,Object> qooAttributesMap;

    public QoOReportMsg(String pipeline_id) {
        this.pipeline_id = pipeline_id;
        this.producer = "UNKNOWN";
        this.qooAttributesMap = new ConcurrentHashMap<>();
    }

    public void setProducer(String producer) {
        this.producer = producer;
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
