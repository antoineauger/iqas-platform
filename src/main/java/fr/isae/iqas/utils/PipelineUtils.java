package fr.isae.iqas.utils;

import akka.actor.ActorContext;
import akka.dispatch.OnComplete;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.model.jsonld.VirtualSensor;
import fr.isae.iqas.model.jsonld.VirtualSensorList;
import fr.isae.iqas.model.quality.QoOAttribute;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.pipelines.*;

import static akka.dispatch.Futures.future;

/**
 * Created by an.auger on 19/04/2017.
 */
public class PipelineUtils {
    public static void setOptionsForIngestPipeline(IngestPipeline pipeline,
                                                   Request incomingRequest,
                                                   FusekiController fusekiController,
                                                   ActorContext context) {
        future(() -> fusekiController._findAllSensorsWithConditions(incomingRequest.getLocation(), incomingRequest.getTopic()), context.dispatcher())
                .onComplete(new OnComplete<VirtualSensorList>() {
                    public void onComplete(Throwable throwable, VirtualSensorList vList) {
                        if (throwable != null) {
                            //log.error("Error when retrieving sensor capabilities. " + throwable.toString());
                        } else {
                            StringBuilder sensorsToKeep = new StringBuilder();
                            for (VirtualSensor v : vList.sensors) {
                                sensorsToKeep.append(v.sensor_id.split("#")[1]).append(";");
                            }
                            sensorsToKeep = new StringBuilder(sensorsToKeep.substring(0, sensorsToKeep.length() - 1));
                            pipeline.setCustomizableParameter("allowed_sensors", sensorsToKeep.toString());
                        }
                    }
                }, context.dispatcher());
    }

    public static void setOptionsForRemoveOutdatedPipeline(RemoveOutdatedPipeline pipeline, Request incomingRequest) {
        if (incomingRequest.getQooConstraints().getIqas_params().containsKey("age_max")) {
            pipeline.setCustomizableParameter("age_max", incomingRequest.getQooConstraints().getIqas_params().get("age_max"));
        }
    }

    public static void setOptionsForThrottlePipeline(ThrottlePipeline pipeline, Request incomingRequest) {
        if (incomingRequest.getQooConstraints().getIqas_params().containsKey("obsRate_max")) {
            pipeline.setCustomizableParameter("obsRate_max", incomingRequest.getQooConstraints().getIqas_params().get("obsRate_max"));
        }
    }

    public static void setOptionsForFilterPipeline(FilterPipeline pipeline, Request incomingRequest) {
        if (incomingRequest.getQooConstraints().getIqas_params().containsKey("threshold_min")) {
            pipeline.setCustomizableParameter("threshold_min", incomingRequest.getQooConstraints().getIqas_params().get("threshold_min"));
        }
        if (incomingRequest.getQooConstraints().getIqas_params().containsKey("threshold_max")) {
            pipeline.setCustomizableParameter("threshold_max", incomingRequest.getQooConstraints().getIqas_params().get("threshold_max"));
        }
    }

    public static void setOptionsForOutputPipeline(OutputPipeline pipeline, Request incomingRequest) {
        StringBuilder interestAttr = new StringBuilder();
        if (incomingRequest.getQooConstraints().getInterested_in().size() > 0) {
            for (QoOAttribute a : incomingRequest.getQooConstraints().getInterested_in()) {
                interestAttr.append(a.toString()).append(";");
            }
            interestAttr = new StringBuilder(interestAttr.substring(0, interestAttr.length() - 1));
            pipeline.setCustomizableParameter("interested_in", interestAttr.toString());
        }
    }
}
