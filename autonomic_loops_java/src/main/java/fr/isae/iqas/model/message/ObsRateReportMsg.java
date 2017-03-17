package fr.isae.iqas.model.message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 27/02/2017.
 */
public class ObsRateReportMsg {
    private String uniquePipelineID;
    private Map<String,Integer> obsRateByTopic;

    public ObsRateReportMsg(String uniquePipelineID) {
        this.uniquePipelineID = uniquePipelineID;
        this.obsRateByTopic = new ConcurrentHashMap<>();
    }

    public String getUniquePipelineID() {
        return uniquePipelineID;
    }

    public Map<String, Integer> getObsRateByTopic() {
        return obsRateByTopic;
    }

    public void setObsRateByTopic(Map<String, Integer> obsRateByTopic) {
        this.obsRateByTopic = obsRateByTopic;
    }
}
