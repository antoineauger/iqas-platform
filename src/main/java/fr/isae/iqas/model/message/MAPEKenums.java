package fr.isae.iqas.model.message;

/**
 * Created by an.auger on 21/02/2017.
 */
public class MAPEKenums {
    public enum SymptomMAPEK {
        NEW,
        UPDATED,
        REMOVED,
        TOO_HIGH,
        TOO_LOW,
        CONNECTION_REPORT
    }

    public enum EntityMAPEK {
        REQUEST,
        PIPELINE,
        OBS_RATE,
        SENSOR,
        KAFKA_TOPIC
    }

    public enum RFCMAPEK {
        CREATE,
        UPDATE,
        REMOVE,
        HEAL,
        RESET
    }

    public enum ActionMAPEK {
        APPLY,
        CREATE,
        RESET,
        DELETE,
        TURN_ON,
        TURN_OFF,
        SENSOR_API
    }
}
