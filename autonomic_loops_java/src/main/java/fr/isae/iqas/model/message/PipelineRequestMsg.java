package fr.isae.iqas.model.message;

/**
 * Created by an.auger on 03/02/2017.
 */

public class PipelineRequestMsg {

    private boolean request;
    private boolean getAllPipelines;
    private String specificPipelineToGet;

    public PipelineRequestMsg() {
        this.request = true;
        this.getAllPipelines = true;
    }

    public PipelineRequestMsg(String pipelineName) {
        this.request = true;
        this.getAllPipelines = false;
        this.specificPipelineToGet = pipelineName;
    }

    // Getters

    public boolean isRequest() {
        return request;
    }

    public boolean isGetAllPipelines() {
        return getAllPipelines;
    }

    public String getSpecificPipelineToGet() {
        return specificPipelineToGet;
    }

}
