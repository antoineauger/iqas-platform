package fr.isae.iqas.utils;

import fr.isae.iqas.config.Config;
import fr.isae.iqas.model.jsonld.VirtualSensorList;
import fr.isae.iqas.model.quality.QoOAttribute;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.pipelines.FilterPipeline;
import fr.isae.iqas.pipelines.OutputPipeline;
import fr.isae.iqas.pipelines.ThrottlePipeline;
import org.apache.jena.ontology.OntModel;

/**
 * Created by an.auger on 19/04/2017.
 */
public class PipelineUtils {
    public static void setOptionsForThrottlePipeline(ThrottlePipeline pipeline, Request incomingRequest) {
        // Params reset
        pipeline.getParams().replace("obsRate_max", String.valueOf(Integer.MAX_VALUE)+"/s");

        if (incomingRequest.getQooConstraints().getIqas_params().containsKey("obsRate_max")) {
            pipeline.setCustomizableParameter("obsRate_max", incomingRequest.getQooConstraints().getIqas_params().get("obsRate_max"));
        }
    }

    public static void setOptionsForFilterPipeline(FilterPipeline pipeline, Request incomingRequest) {
        // Params reset
        pipeline.getParams().replace("threshold_min", String.valueOf(Double.MIN_VALUE));
        pipeline.getParams().replace("threshold_max", String.valueOf(Double.MAX_VALUE));

        if (incomingRequest.getQooConstraints().getIqas_params().containsKey("threshold_min")) {
            pipeline.setCustomizableParameter("threshold_min", incomingRequest.getQooConstraints().getIqas_params().get("threshold_min"));
        }
        if (incomingRequest.getQooConstraints().getIqas_params().containsKey("threshold_max")) {
            pipeline.setCustomizableParameter("threshold_max", incomingRequest.getQooConstraints().getIqas_params().get("threshold_max"));
        }
    }

    public static void setOptionsForOutputPipeline(OutputPipeline pipeline,
                                                   Config iqasConfig,
                                                   Request incomingRequest,
                                                   VirtualSensorList virtualSensorList,
                                                   OntModel qooBaseModel) {
        // Params reset
        pipeline.getParams().replace("age_max", "unset");
        pipeline.getParams().replace("interested_in", "");

        StringBuilder interestAttr = new StringBuilder();
        if (incomingRequest.getQooConstraints().getInterested_in().size() > 0) {
            for (QoOAttribute a : incomingRequest.getQooConstraints().getInterested_in()) {
                interestAttr.append(a.toString()).append(";");
            }
            interestAttr = new StringBuilder(interestAttr.substring(0, interestAttr.length() - 1));
            pipeline.setCustomizableParameter("interested_in", interestAttr.toString());
        }
        if (incomingRequest.getQooConstraints().getIqas_params().containsKey("age_max")) {
            pipeline.setCustomizableParameter("age_max", incomingRequest.getQooConstraints().getIqas_params().get("age_max"));
        }
        pipeline.setSensorContext(iqasConfig, virtualSensorList, qooBaseModel);
    }
}
