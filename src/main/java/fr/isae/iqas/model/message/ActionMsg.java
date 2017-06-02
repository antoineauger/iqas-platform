package fr.isae.iqas.model.message;

import java.sql.Timestamp;

import static fr.isae.iqas.model.message.MAPEKenums.ActionMAPEK;
import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK;

/**
 * Created by an.auger on 02/06/2017.
 */
public class ActionMsg {
    private final Timestamp creationDate;
    private final ActionMAPEK action;
    private final EntityMAPEK about;

    public ActionMsg(ActionMAPEK action, EntityMAPEK about) {
        this.creationDate = new Timestamp(System.currentTimeMillis());
        this.action = action;
        this.about = about;
    }

    public Timestamp getCreationDate() {
        return creationDate;
    }

    public ActionMAPEK getAction() {
        return action;
    }

    public EntityMAPEK getAbout() {
        return about;
    }
}

