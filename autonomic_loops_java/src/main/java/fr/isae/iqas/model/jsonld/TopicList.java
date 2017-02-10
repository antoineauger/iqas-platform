package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

import java.util.List;

/**
 * Created by an.auger on 10/02/2017.
 */
@JsonldType("@list")
public class TopicList {
    public List<Topic> topics;
}
