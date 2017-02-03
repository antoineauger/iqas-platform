package fr.isae.iqas.model.messages;

/**
 * Created by an.auger on 14/09/2016.
 */
public class SymptomMsg {
    String type;
    String symptomName;
    Long timestamp;

    public SymptomMsg() {
        timestamp = System.currentTimeMillis();
    }
}