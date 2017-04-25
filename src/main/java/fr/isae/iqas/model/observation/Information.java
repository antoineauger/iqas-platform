package fr.isae.iqas.model.observation;

import fr.isae.iqas.model.jsonld.VirtualSensor;

import java.sql.Timestamp;

/**
 * Created by an.auger on 08/02/2017.
 */
public class Information extends RawData {
    private Context sensorContext;

    public Information(RawData rawData) {
        super(rawData);
        this.sensorContext = new Context();
    }

    public Information(String date, String value, String producer, String timestamps, String stepName, long timestamp) {
        super(date, value, producer, timestamps, stepName, timestamp);
        this.sensorContext = new Context();
    }

    public Information(String date, String value, String producer, String timestamps) {
        super(date, value, producer, timestamps);
        this.sensorContext = new Context();
    }

    public Information(Timestamp date, Double value, String producer, String timestamps) {
        super(date, value, producer, timestamps);
        this.sensorContext = new Context();
    }

    public Information(Information information) {
        super(information.getDate(), information.getValue(), information.getProducer(), information.getTimestamps(), information.getQoOAttributeValues());
        this.sensorContext = new Context();
    }

    public Context getSensorContext() {
        return sensorContext;
    }

    public void setSensorContext(Context sensorContext) {
        this.sensorContext = sensorContext;
    }

    public void setContext(VirtualSensor virtualSensor) {
        this.sensorContext = new Context(virtualSensor);
    }
}
