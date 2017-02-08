package fr.isae.iqas.model.entity;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldProperty;

/**
 * Created by an.auger on 06/02/2017.
 */

public class ServiceEndpoint {
    @JsonldProperty("http://purl.oclc.org/NET/ssnx/ssn#Property")
    public String topic;

    @JsonldProperty("http://purl.oclc.org/NET/UNIS/fiware/iot-lite#endpoint")
    public String url;
}
