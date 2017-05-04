package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldId;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldProperty;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

import java.util.List;

/**
 * Created by an.auger on 04/05/2017.
 */
@JsonldType("http://isae.fr/iqas/qoo-ontology#QoOPipeline")
public class QoOPipeline {
    @JsonldId
    public String pipeline;

    @JsonldProperty("http://isae.fr/iqas/qoo-ontology#allowsToSet")
    public List<QoOCustomizableParam> customizable_params;
}
