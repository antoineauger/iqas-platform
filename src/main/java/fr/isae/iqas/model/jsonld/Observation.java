package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldProperty;

/**
 * Created by an.auger on 27/04/2017.
 */
public class Observation {
    @JsonldProperty("http://purl.oclc.org/NET/ssnx/ssn#FeatureOfInterest")
    public String featureOfInterest;

    @JsonldProperty("http://purl.oclc.org/NET/ssnx/ssn#Property")
    public String observedProperty;
}
