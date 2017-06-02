package fr.isae.iqas.model.message;

import fr.isae.iqas.kafka.RequestMapping;
import fr.isae.iqas.model.quality.QoOAttribute;
import fr.isae.iqas.model.request.HealRequest;

import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK;
import static fr.isae.iqas.model.message.MAPEKenums.RFCMAPEK;

/**
 * Created by an.auger on 02/06/2017.
 */
public class RFCMsgQoOAttr extends RFCMsg {
    private final QoOAttribute qoOAttribute;
    private final RequestMapping oldRequestMapping;
    private final HealRequest healRequest;
    private final RequestMapping newRequestMapping;

    public RFCMsgQoOAttr(RFCMAPEK rfc, EntityMAPEK about, HealRequest healRequest, RequestMapping oldRequestMapping, RequestMapping newRequestMapping) {
        super(rfc, about);
        this.qoOAttribute = healRequest.getConcernedAttr();
        this.oldRequestMapping = oldRequestMapping;
        this.newRequestMapping = newRequestMapping;
        this.healRequest = healRequest;
    }

    public QoOAttribute getQoOAttribute() {
        return qoOAttribute;
    }

    public RequestMapping getOldRequestMapping() {
        return oldRequestMapping;
    }

    public HealRequest getHealRequest() {
        return healRequest;
    }

    public RequestMapping getNewRequestMapping() {
        return newRequestMapping;
    }
}
