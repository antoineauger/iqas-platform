package fr.isae.iqas.model.message;

import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK;
import static fr.isae.iqas.model.message.MAPEKenums.RFCMAPEK;

/**
 * Created by an.auger on 02/06/2017.
 */
public class RFCMsg extends MAPEKMsg {
    private RFCMAPEK rfc;

    public RFCMsg(RFCMAPEK rfc, EntityMAPEK about) {
        super(about);
        this.rfc = rfc;
    }

    public RFCMAPEK getRfc() {
        return rfc;
    }
}
