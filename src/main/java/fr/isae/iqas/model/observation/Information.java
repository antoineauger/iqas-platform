package fr.isae.iqas.model.observation;

import java.sql.Timestamp;

/**
 * Created by an.auger on 08/02/2017.
 */
public class Information extends RawData {


    public Information(RawData rawData) {
        super(rawData);
    }

    public Information(String date, String value, String producer, String timestamps, String stepName, long timestamp) {
        super(date, value, producer, timestamps, stepName, timestamp);
    }

    public Information(String date, String value, String producer, String timestamps) {
        super(date, value, producer, timestamps);
    }

    public Information(Timestamp date, Double value, String producer, String timestamps) {
        super(date, value, producer, timestamps);
    }

    public Information(Information information) {
        super(information.getDate(), information.getValue(), information.getProducer(), information.getTimestamps(), information.getQoOAttributeValues());
    }

    /**
     * Getters and setters for a specific QoO attribute
     */

    /*public void setQoOAttribute(String attribute, String value) {
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
    }*/

    /**
     * Getters and setters for attributes
     */

    /*public Map<QoOAttribute, String> getQoOAttributeValues() {
        return qoOAttributeValues;
    }

    public void setQoOAttributeValues(Map<QoOAttribute, String> qoOAttributeValues) {
        this.qoOAttributeValues = qoOAttributeValues;
    }

    public void setQoOAttributeValuesFromJSON(Map<String, String> qoOAttributeValues) {
        qoOAttributeValues.forEach( (k,v) -> {
            this.qoOAttributeValues.put(QoOAttribute.valueOf(k), v);
        });
    }*/
}
