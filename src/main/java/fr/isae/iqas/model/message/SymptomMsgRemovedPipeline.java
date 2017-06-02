package fr.isae.iqas.model.message;

/**
 * Created by an.auger on 01/06/2017.
 */
public class SymptomMsgRemovedPipeline extends SymptomMsg {

    private final String uniqueIDPipeline;

    public SymptomMsgRemovedPipeline(MAPEKenums.SymptomMAPEK symptom, MAPEKenums.EntityMAPEK about, String uniqueIDRemovedPipeline) {
        super(symptom, about);
        this.uniqueIDPipeline = uniqueIDRemovedPipeline;
    }

    public String getUniqueIDPipeline() {
        return uniqueIDPipeline;
    }
}
