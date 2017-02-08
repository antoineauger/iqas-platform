package fr.isae.iqas.model.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.isae.iqas.pipelines.IPipeline;

/**
 * Created by an.auger on 06/02/2017.
 */
public class Pipeline {
    private String pipeline_id;
    private String name;
    private IPipeline pipelineObject;

    public Pipeline(String pipeline_id, String name, IPipeline pipelineObject) {
        this.pipeline_id = pipeline_id;
        this.name = name;
        this.pipelineObject = pipelineObject;
    }

    @JsonProperty("pipeline_name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("pipeline_object")
    public IPipeline getPipelineObject() {
        return pipelineObject;
    }

    public void setPipelineObject(IPipeline pipelineObject) {
        this.pipelineObject = pipelineObject;
    }

    public String getPipeline_id() {
        return pipeline_id;
    }

    public void setPipeline_id(String pipeline_id) {
        this.pipeline_id = pipeline_id;
    }
}
