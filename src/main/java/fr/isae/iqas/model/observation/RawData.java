package fr.isae.iqas.model.observation;

import fr.isae.iqas.model.quality.QoOAttribute;

import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 08/02/2017.
 */

public class RawData {
    private Timestamp date;
    private Double value;
    private String producer;
    private String timestamps;
    private Map<QoOAttribute, String> qoOAttributeValues;

    public RawData(String date, String value, String producer, String timestamps) {
        this.date = new Timestamp(Long.valueOf(date));
        this.value = Double.valueOf(value);
        this.producer = producer;
        this.timestamps = timestamps;
        this.qoOAttributeValues = new ConcurrentHashMap<>();
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

        this.qoOAttributeValues = new ConcurrentHashMap<>();
    }

    public RawData(Timestamp date, Double value, String producer, String timestamps) {
        this.date = date;
        this.value = value;
        this.producer = producer;
        this.timestamps = timestamps;
        this.qoOAttributeValues = new ConcurrentHashMap<>();
    }

    public RawData(Timestamp date, Double value, String producer, String timestamps, Map<QoOAttribute, String> qoOAttributeValues) {
        this.date = date;
        this.value = value;
        this.producer = producer;
        this.timestamps = timestamps;
        this.qoOAttributeValues = qoOAttributeValues;
    }

    public RawData(RawData rawData) {
        this.date = rawData.getDate();
        this.value = rawData.getValue();
        this.producer = rawData.getProducer();
        this.timestamps = rawData.getTimestamps();

        if (rawData.getQoOAttributeValues() == null) {
            this.qoOAttributeValues = new ConcurrentHashMap<>();
        }
        else {
            this.qoOAttributeValues = rawData.getQoOAttributeValues();
        }
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

    public void addTimestamp(String stepName, long timestamp) {
        if (this.timestamps.equals("")) {
            this.timestamps = this.timestamps.concat(stepName + ":" + String.valueOf(timestamp));
        }
        else {
            this.timestamps = this.timestamps.concat(";" + stepName + ":" + String.valueOf(timestamp));
        }
    }

    /**
     * Getters and setters for a specific QoO attribute
     */

    public void setQoOAttribute(String attribute, String value) {
        qoOAttributeValues.put(QoOAttribute.valueOf(attribute), value);
    }

    public String getQoOAttribute(String attribute) {
        String attributeValue = "";
        if (qoOAttributeValues.containsKey(QoOAttribute.valueOf(attribute))) {
            attributeValue = qoOAttributeValues.get(QoOAttribute.valueOf(attribute));
        }
        return attributeValue;
    }

    public void setQoOAttribute(QoOAttribute attribute, String value) {
        qoOAttributeValues.put(attribute, value);
    }

    public String getQoOAttribute(QoOAttribute attribute) {
        String attributeValue = "";
        if (qoOAttributeValues.containsKey(attribute)) {
            attributeValue = qoOAttributeValues.get(attribute);
        }
        return attributeValue;
    }

    /**
     * Getters and setters for attributes
     */

    public Map<QoOAttribute, String> getQoOAttributeValues() {
        return qoOAttributeValues;
    }

    public void setQoOAttributeValues(Map<QoOAttribute, String> qoOAttributeValues) {
        this.qoOAttributeValues = qoOAttributeValues;
    }

    public void setQoOAttributeValuesFromJSON(Map<String, String> qoOAttributeValues) {
        qoOAttributeValues.forEach( (k,v) -> this.qoOAttributeValues.put(QoOAttribute.valueOf(k), v));
    }
}
