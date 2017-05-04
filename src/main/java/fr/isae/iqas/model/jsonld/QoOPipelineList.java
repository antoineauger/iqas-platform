package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

import java.util.List;

/**
 * Created by an.auger on 04/05/2017.
 */
@JsonldType("@list")
public class QoOPipelineList {
    public List<QoOPipeline> qoOPipelines;
}
