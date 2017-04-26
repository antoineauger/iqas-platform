package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldId;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

/**
 * Created by an.auger on 03/02/2017.
 */
@JsonldType("http://purl.oclc.org/NET/ssnx/ssn#Sensor")
public class VirtualSensor {
    @JsonldId
    public String sensor_id;

    public Location location;

    public ServiceEndpoint endpoint;
}