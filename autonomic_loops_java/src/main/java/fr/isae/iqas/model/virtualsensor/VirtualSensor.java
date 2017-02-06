package fr.isae.iqas.model.virtualsensor;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldId;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldProperty;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

import java.util.List;

/**
 * Created by an.auger on 03/02/2017.
 */
@JsonldType("http://purl.oclc.org/NET/ssnx/ssn#Sensor")
public class VirtualSensor {
    @JsonldId
    public  String sensor_id;

    @JsonldProperty("http://www.w3.org/2003/01/geo/wgs84_pos#location")
    public Location location;

    public List<ServiceEndpoint> endpoints;
}