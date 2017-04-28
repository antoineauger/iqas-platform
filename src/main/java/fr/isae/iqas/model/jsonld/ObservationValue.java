package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldProperty;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

/**
 * Created by an.auger on 27/04/2017.
 */
@JsonldType("http://purl.oclc.org/NET/ssnx/ssn#ObservationValue")
public class ObservationValue {
    @JsonldProperty("http://purl.oclc.org/NET/UNIS/fiware/iot-lite#hasQuantityKind")
    public String hasQuantityKind;

    @JsonldProperty("http://purl.oclc.org/NET/UNIS/fiware/iot-lite#hasUnit")
    public String hasUnit;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#obsLevelValue")
    public String obsLevelValue;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#obsStrValue")
    public String obsStrValue;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#obsDateValue")
    public String obsDateValue;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#obsTimestampsValue")
    public String obsTimestampsValue;
}
