package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldProperty;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

/**
 * Created by an.auger on 06/02/2017.
 */

@JsonldType("http://purl.oclc.org/NET/UNIS/fiware/iot-lite#Service")
public class ServiceEndpoint {
    @JsonldProperty("http://purl.oclc.org/NET/UNIS/fiware/iot-lite#interfaceDescription")
    public String description;

    @JsonldProperty("http://purl.oclc.org/NET/UNIS/fiware/iot-lite#interfaceType")
    public String if_type;

    @JsonldProperty("http://purl.oclc.org/NET/UNIS/fiware/iot-lite#endpoint")
    public String url;
}
