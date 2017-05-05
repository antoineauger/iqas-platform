package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldProperty;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

import java.util.List;

/**
 * Created by an.auger on 15/02/2017.
 */

@JsonldType("http://isae.fr/iqas/qoo-ontology#QoOCustomizableParameter")
public class QoOCustomizableParam {
    public String param_name;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#documentation")
    public String documentation;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#paramType")
    public String paramType;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#paramMinValue")
    public String paramMinValue;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#paramMaxValue")
    public String paramMaxValue;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#paramInitialValue")
    public String paramInitialValue;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#has")
    public List<QoOEffect> has;
}
