package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldId;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

/**
 * Created by an.auger on 10/02/2017.
 */
@JsonldType("http://purl.oclc.org/NET/ssnx/ssn#Property")
public class Topic {
    @JsonldId
    public String topic;
}
