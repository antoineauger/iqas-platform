package fr.isae.iqas.model.observation;

import java.sql.Timestamp;

/**
 * Created by an.auger on 08/02/2017.
 */
public class RawData {

    private Timestamp date;
    private Double value;
    private String producer;
    private String timestamps;

    public RawData(String date, String value, String producer, String timestamps) {
        this.date = new Timestamp(Long.valueOf(date));
        this.value = Double.valueOf(value);
        this.producer = producer;
        this.timestamps = timestamps;
    }

    public RawData(String date, String value, String producer, String timestamps, String stepName, long timestamp) {
        this.date = new Timestamp(Long.valueOf(date));
        this.value = Double.valueOf(value);
        this.producer = producer;
        this.timestamps = timestamps;

        if (this.timestamps == null || this.timestamps.equals("")) {
            this.timestamps = stepName + ":" + String.valueOf(timestamp);
        }
        else {
            this.timestamps = this.timestamps.concat(";" + stepName + ":" + String.valueOf(timestamp));
        }
    }

    public RawData(Timestamp date, Double value, String producer, String timestamps) {
        this.date = date;
        this.value = value;
        this.producer = producer;
        this.timestamps = timestamps;
    }

    public RawData(RawData rawData) {
        this.date = rawData.getDate();
        this.value = rawData.getValue();
        this.producer = rawData.getProducer();
        this.timestamps = rawData.getTimestamps();
    }

    /**
     * Getters and setters for attributes
     */

    public Timestamp getDate() {
        return date;
    }

    public void setDate(Timestamp date) {
        this.date = date;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public String getProducer() {
        return producer;
    }

    public void setProducer(String producer) {
        this.producer = producer;
    }

    public String getTimestamps() {
        return timestamps;
    }

    public void addTimestamp(String stepName, String timestampString) {
        if (this.timestamps.equals("")) {
            this.timestamps = this.timestamps.concat(stepName + ":" + timestampString);
        }
        else {
            this.timestamps = this.timestamps.concat(";" + stepName + ":" + timestampString);
        }
    }
}
