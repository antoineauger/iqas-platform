package fr.isae.iqas.model.message;

import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK;
import static fr.isae.iqas.model.message.MAPEKenums.SymptomMAPEK;

/**
 * Created by an.auger on 01/06/2017.
 */
public class SymptomMsg extends MAPEKMsg {
    private SymptomMAPEK symptom;

    public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about) {
        super(about);
        this.symptom = symptom;
    }

    public MAPEKenums.SymptomMAPEK getSymptom() {
        return symptom;
    }
}
