package fr.isae.iqas.model.messages;

/**
 * Created by an.auger on 14/09/2016.
 */
public class SymptomMsg {
    private String type;
    private String symptomName;
    private Long timestamp;

    public SymptomMsg() {
        timestamp = System.currentTimeMillis();
    }
}
