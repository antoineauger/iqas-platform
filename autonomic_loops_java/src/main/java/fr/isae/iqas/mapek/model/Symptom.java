package fr.isae.iqas.mapek.model;

import java.sql.Time;

/**
 * Created by an.auger on 14/09/2016.
 */
public class Symptom {
    String type;
    String symptomName;
    Long timestamp;

    public Symptom() {
        timestamp = System.currentTimeMillis();
    }
}
