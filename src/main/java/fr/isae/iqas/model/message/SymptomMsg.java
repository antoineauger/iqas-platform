package fr.isae.iqas.model.message;

import java.sql.Timestamp;

import static fr.isae.iqas.model.message.MAPEKenums.*;

/**
 * Created by an.auger on 01/06/2017.
 */
public class SymptomMsg {
    private Timestamp creationDate;
    private SymptomMAPEK symptom;
    private EntityMAPEK about;

    public SymptomMsg(SymptomMAPEK symptom, EntityMAPEK about) {
        this.creationDate = new Timestamp(System.currentTimeMillis());
        this.symptom = symptom;
        this.about = about;
    }

    public Timestamp getCreationDate() {
        return creationDate;
    }

    public MAPEKenums.SymptomMAPEK getSymptom() {
        return symptom;
    }

    public MAPEKenums.EntityMAPEK getAbout() {
        return about;
    }
}
