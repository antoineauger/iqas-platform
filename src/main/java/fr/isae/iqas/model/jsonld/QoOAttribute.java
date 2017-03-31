package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldId;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldProperty;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

/**
 * Created by an.auger on 15/02/2017.
 */
@JsonldType("http://isae.fr/iqas/qoo-ontology#QoOAttribute")
public class QoOAttribute {
    @JsonldId
    public String name;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#shouldBe")
    public String should_be;
}
