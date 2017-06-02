package fr.isae.iqas.model.message;

/**
 * Created by an.auger on 01/06/2017.
 */
public class SymptomMsgPipelineCreation extends SymptomMsg {
    private final String uniqueIDPipeline;
    private final String requestID;

    public SymptomMsgPipelineCreation(MAPEKenums.SymptomMAPEK symptom, MAPEKenums.EntityMAPEK about, String uniqueIDPipeline, String requestID) {
        super(symptom, about);
        this.uniqueIDPipeline = uniqueIDPipeline;
        this.requestID = requestID;
    }

    public String getUniqueIDPipeline() {
        return uniqueIDPipeline;
    }

    public String getRequestID() {
        return requestID;
    }
}
