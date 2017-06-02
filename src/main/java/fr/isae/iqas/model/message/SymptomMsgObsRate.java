package fr.isae.iqas.model.message;

import java.util.List;

/**
 * Created by an.auger on 01/06/2017.
 */
public class SymptomMsgObsRate extends SymptomMsg {
    private final String uniqueIDPipeline;
    private final List<String> concernedRequests;

    public SymptomMsgObsRate(MAPEKenums.SymptomMAPEK symptom, MAPEKenums.EntityMAPEK about, String concernedUniqueIDPipeline, List<String> concernedRequests) {
        super(symptom, about);
        this.uniqueIDPipeline = concernedUniqueIDPipeline;
        this.concernedRequests = concernedRequests;
    }

    public String getUniqueIDPipeline() {
        return uniqueIDPipeline;
    }

    public List<String> getConcernedRequests() {
        return concernedRequests;
    }
}
