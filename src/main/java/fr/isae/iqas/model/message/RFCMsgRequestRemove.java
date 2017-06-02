package fr.isae.iqas.model.message;

import fr.isae.iqas.model.request.Request;

import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK;
import static fr.isae.iqas.model.message.MAPEKenums.RFCMAPEK;

/**
 * Created by an.auger on 02/06/2017.
 */
public class RFCMsgRequestRemove extends RFCMsg{
    private final Request request;
    private final String associatedRequest_id;

    public RFCMsgRequestRemove(RFCMAPEK rfc, EntityMAPEK about, Request request) {
        super(rfc, about);
        this.request = request;
        this.associatedRequest_id = request.getRequest_id();
    }

    public Request getRequest() {
        return request;
    }

    public String getAssociatedRequest_id() {
        return associatedRequest_id;
    }
}
