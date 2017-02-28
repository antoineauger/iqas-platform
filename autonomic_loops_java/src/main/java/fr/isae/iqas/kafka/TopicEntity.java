package fr.isae.iqas.kafka;

import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.pipelines.Pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by an.auger on 28/02/2017.
 */
public class TopicEntity {
    private String name;
    private String forTopic;
    private ObservationLevel observationLevel;
    private boolean isSinkForSensors;
    private boolean isSinkForApplications;

    List<TopicEntity> children;
    Map<TopicEntity, Pipeline> enforcedPipelines;

    public TopicEntity(String name, String forTopic) {
        this.name = name;
        this.forTopic = forTopic;
        this.isSinkForSensors = true;
        this.isSinkForApplications = false;
        this.observationLevel = ObservationLevel.INFORMATION; //TODO make this dynamic

        this.children = new ArrayList<>();
        this.enforcedPipelines = new HashMap<>();
    }

    public TopicEntity(String name) {
        this.name = name;
        this.isSinkForSensors = false;
        this.isSinkForApplications = true;
        this.observationLevel = ObservationLevel.INFORMATION; //TODO make this dynamic

        this.children = new ArrayList<>();
        this.enforcedPipelines = new HashMap<>();
    }
}
