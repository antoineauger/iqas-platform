package fr.isae.iqas.model.observation;

import java.sql.Timestamp;

/**
 * Created by an.auger on 08/02/2017.
 */
public class RawData {

    private Timestamp timestamp;
    private Double value;
    private String producer;

    public RawData(String timestamp, String value, String producer) {
        this.timestamp = new Timestamp(Long.valueOf(timestamp));
        this.value = Double.valueOf(value);
        this.producer = producer;
    }

    public RawData(Timestamp timestamp, Double value, String producer) {
        this.timestamp = timestamp;
        this.value = value;
        this.producer = producer;
    }

    public RawData(RawData rawData) {
        this.timestamp = rawData.getTimestamp();
        this.value = rawData.getValue();
        this.producer = rawData.getProducer();
    }

    /**
     * Getters and setters for attributes
     */

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
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
}
