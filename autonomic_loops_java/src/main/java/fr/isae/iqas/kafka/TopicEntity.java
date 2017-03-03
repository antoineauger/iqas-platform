package fr.isae.iqas.kafka;

import fr.isae.iqas.model.observation.ObservationLevel;

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
    private String forApplication;
    private ObservationLevel observationLevel;
    private boolean isSource;
    private boolean isSink;

    private List<TopicEntity> parents;
    private List<TopicEntity> children;
    private Map<TopicEntity, String> enforcedPipelines; // EnforcedPipelines with uniqueID

    public TopicEntity(String name) {
        this.name = name;
        this.isSource = false;
        this.isSink = false;
        this.observationLevel = ObservationLevel.INFORMATION; //TODO make this dynamic

        this.parents = new ArrayList<>();
        this.children = new ArrayList<>();
        this.enforcedPipelines = new HashMap<>();
    }

    public void setSource(String forTopic) {
        this.isSource = true;
        this.forTopic = forTopic;
    }

    public void setSink(String forApplication) {
        this.isSink = true;
        this.forApplication = forApplication;
    }

    /*public TopicEntity(String name, String forTopic) {
        this.name = name;
        this.forTopic = forTopic;
        this.isSource = true;
        this.isSink = false;
        this.observationLevel = ObservationLevel.INFORMATION; //TODO make this dynamic

        this.parents = new ArrayList<>();
        this.children = new ArrayList<>();
        this.enforcedPipelines = new HashMap<>();
    }

    public TopicEntity(String name) {
        this.name = name;
        this.isSource = false;
        this.isSink = false;
        this.observationLevel = ObservationLevel.INFORMATION; //TODO make this dynamic

        this.parents = new ArrayList<>();
        this.children = new ArrayList<>();
        this.enforcedPipelines = new HashMap<>();
    }

    public TopicEntity(String name, boolean isFinalSink) {
        this.name = name;
        this.isSource = false;
        this.isSink = isFinalSink;
        this.observationLevel = ObservationLevel.INFORMATION; //TODO make this dynamic

        this.parents = new ArrayList<>();
        this.children = new ArrayList<>();
        this.enforcedPipelines = new HashMap<>();
    }*/

    public String getName() {
        return name;
    }

    public boolean isSource() {
        return isSource;
    }

    public boolean isSink() {
        return isSink;
    }

    public ObservationLevel getObservationLevel() {
        return observationLevel;
    }

    public List<TopicEntity> getParents() {
        return parents;
    }

    public List<TopicEntity> getChildren() {
        return children;
    }

    public Map<TopicEntity, String> getEnforcedPipelines() {
        return enforcedPipelines;
    }

    public String getForTopic() {
        return forTopic;
    }

    public String getForApplication() {
        return forApplication;
    }
}
