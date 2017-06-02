package fr.isae.iqas.model.message;

import fr.isae.iqas.model.request.Request;

/**
 * Created by an.auger on 01/06/2017.
 */
public class SymptomMsgRequest extends SymptomMsg {
    private Request attachedRequest;

    public SymptomMsgRequest(MAPEKenums.SymptomMAPEK symptom, MAPEKenums.EntityMAPEK about, Request attachedRequest) {
        super(symptom, about);
        this.attachedRequest = attachedRequest;
    }

    public Request getAttachedRequest() {
        return attachedRequest;
    }
}
