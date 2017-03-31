package fr.isae.iqas.model.observation;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

import java.sql.Timestamp;

/**
 * Created by an.auger on 08/02/2017.
 */
@JsonldType("http://purl.oclc.org/NET/ssnx/ssn#SensorOutput")
public class Knowledge extends Information {

    public Knowledge(Information information) {
        super(information);
    }

    public Knowledge(RawData rawData) {
        super(rawData);
    }

    public Knowledge(String date, String value, String producer, String timestamps) {
        super(date, value, producer, timestamps);
    }

    public Knowledge(Timestamp date, Double value, String producer, String timestamps) {
        super(date, value, producer, timestamps);
    }


}
