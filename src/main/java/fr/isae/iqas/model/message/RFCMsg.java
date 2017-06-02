package fr.isae.iqas.model.message;

import java.sql.Timestamp;

import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK;
import static fr.isae.iqas.model.message.MAPEKenums.RFCMAPEK;

/**
 * Created by an.auger on 02/06/2017.
 */
public class RFCMsg {
    private Timestamp creationDate;
    private RFCMAPEK rfc;
    private EntityMAPEK about;

    public RFCMsg(RFCMAPEK rfc, EntityMAPEK about) {
        this.creationDate = new Timestamp(System.currentTimeMillis());
        this.rfc = rfc;
        this.about = about;
    }


    public Timestamp getCreationDate() {
        return creationDate;
    }

    public RFCMAPEK getRfc() {
        return rfc;
    }

    public EntityMAPEK getAbout() {
        return about;
    }
}
