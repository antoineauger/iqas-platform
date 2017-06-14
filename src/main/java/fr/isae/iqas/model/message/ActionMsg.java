package fr.isae.iqas.model.message;

import static fr.isae.iqas.model.message.MAPEKenums.ActionMAPEK;
import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK;

/**
 * Created by an.auger on 02/06/2017.
 */
public class ActionMsg extends MAPEKMsg {
    private final ActionMAPEK action;

    public ActionMsg(ActionMAPEK action, EntityMAPEK about) {
        super(about);
        this.action = action;
    }

    public ActionMAPEK getAction() {
        return action;
    }
}

