package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldProperty;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

/**
 * Created by an.auger on 29/03/2017.
 */
@JsonldType("http://isae.fr/iqas/qoo-ontology#QoOEffect")
public class QoOEffect {
    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#paramVariation")
    public String paramVariation;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#qooAttributeVariation")
    public String qooAttributeVariation;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#QoOAttribute")
    public String impacts;
}
