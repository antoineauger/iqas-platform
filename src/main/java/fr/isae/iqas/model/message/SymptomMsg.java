package fr.isae.iqas.model.message;

import java.sql.Timestamp;

/**
 * Created by an.auger on 01/06/2017.
 */
public class SymptomMsg {
    private Timestamp creationDate;
    private MAPEKenums.SymptomMAPEK symptom;
    private MAPEKenums.EntityMAPEK about;

    public SymptomMsg(MAPEKenums.SymptomMAPEK symptom, MAPEKenums.EntityMAPEK about) {
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
