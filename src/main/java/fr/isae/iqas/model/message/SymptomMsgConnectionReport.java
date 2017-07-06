package fr.isae.iqas.model.message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 01/06/2017.
 */
public class SymptomMsgConnectionReport extends SymptomMsg {
    private final Map<String, Boolean> connectedSensors;

    public SymptomMsgConnectionReport(MAPEKenums.SymptomMAPEK symptom, MAPEKenums.EntityMAPEK about, Map<String, Boolean> connectedSensors) {
        super(symptom, about);
        this.connectedSensors = new ConcurrentHashMap<>(connectedSensors);
    }

    public Map<String, Boolean> getConnectedSensors() {
        return connectedSensors;
    }
}
