package fr.isae.iqas.model.message;

import java.sql.Timestamp;

import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK;

/**
 * Created by an.auger on 14/06/2017.
 */
public class MAPEKMsg {
    private final Timestamp creationDate;
    private final EntityMAPEK about;

    public MAPEKMsg(EntityMAPEK about) {
        this.creationDate = new Timestamp(System.currentTimeMillis());
        this.about = about;
    }

    public Timestamp getCreationDate() {
        return creationDate;
    }

    public EntityMAPEK getAbout() {
        return about;
    }
}
