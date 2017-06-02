package fr.isae.iqas.model.message;

import fr.isae.iqas.kafka.RequestMapping;
import fr.isae.iqas.model.request.Request;

import static fr.isae.iqas.model.message.MAPEKenums.*;

/**
 * Created by an.auger on 02/06/2017.
 */
public class RFCMsgRequestCreate extends RFCMsg {
    private final Request request;
    private final RequestMapping newRequestMapping;
    private final String associatedRequest_id;

    public RFCMsgRequestCreate(RFCMAPEK rfc, EntityMAPEK about, Request request, RequestMapping newRequestMapping) {
        super(rfc, about);
        this.request = request;
        this.newRequestMapping = newRequestMapping;
        this.associatedRequest_id = request.getRequest_id();
    }

    public Request getRequest() {
        return request;
    }

    public RequestMapping getNewRequestMapping() {
        return newRequestMapping;
    }

    public String getAssociatedRequest_id() {
        return associatedRequest_id;
    }
}
