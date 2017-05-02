package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldProperty;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

/**
 * Created by an.auger on 29/03/2017.
 */

@JsonldType("http://purl.oclc.org/NET/ssnx/ssn#MeasurementRange")
public class SensorCapability {
    public String sensor_id;
    public String topic;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#hasMinValue")
    public String min_value;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#hasMaxValue")
    public String max_value;
}
