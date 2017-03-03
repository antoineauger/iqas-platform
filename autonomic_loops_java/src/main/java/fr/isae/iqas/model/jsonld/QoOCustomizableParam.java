package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldProperty;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

/**
 * Created by an.auger on 15/02/2017.
 */

@JsonldType("http://isae.fr/iqas/qoo-ontology#QoOCustomizableParameter")
public class QoOCustomizableParam {
    public String param_name;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#documentation")
    public String details;
}