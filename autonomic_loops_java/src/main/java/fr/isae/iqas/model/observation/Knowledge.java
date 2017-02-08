package fr.isae.iqas.model.observation;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

import java.sql.Timestamp;

/**
 * Created by an.auger on 08/02/2017.
 */
@JsonldType("http://purl.oclc.org/NET/ssnx/ssn#SensorOutput")
public class Knowledge extends Information {
    private Timestamp timestamp;
    private Double value;
    private String producer;

    public Knowledge(Information information) {
        super(information);
        this.timestamp = getTimestamp();
        this.value = getValue();
        this.producer = getProducer();
    }

    public Knowledge(RawData rawData) {
        super(rawData);
        this.timestamp = getTimestamp();
        this.value = getValue();
        this.producer = getProducer();
    }

    public Knowledge(String timestamp, String value, String producer) {
        super(timestamp, value, producer);
        this.timestamp = getTimestamp();
        this.value = getValue();
        this.producer = getProducer();
    }

    public Knowledge(Timestamp timestamp, Double value, String producer) {
        super(timestamp, value, producer);
        this.timestamp = getTimestamp();
        this.value = getValue();
        this.producer = getProducer();
    }


}
