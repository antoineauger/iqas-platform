package fr.isae.iqas.model.observation;

import fr.isae.iqas.model.quality.QoOAttribute;

import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 08/02/2017.
 */
public class Information extends RawData {
    private Map<QoOAttribute, String> qoOAttributeValues;

    public Information(RawData rawData) {
        super(rawData);
        this.qoOAttributeValues = new ConcurrentHashMap<>();
    }

    public Information(String timestamp, String value, String producer) {
        super(timestamp, value, producer);
        this.qoOAttributeValues = new ConcurrentHashMap<>();
    }

    public Information(Timestamp timestamp, Double value, String producer) {
        super(timestamp, value, producer);
        this.qoOAttributeValues = new ConcurrentHashMap<>();
    }

    public Information(Information information) {
        super(information.getTimestamp(), information.getValue(), information.getProducer());
        this.qoOAttributeValues = information.getQoOAttributeValues();
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
        qoOAttributeValues.forEach( (k,v) -> {
            this.qoOAttributeValues.put(QoOAttribute.valueOf(k), v);
        });
    }
}
