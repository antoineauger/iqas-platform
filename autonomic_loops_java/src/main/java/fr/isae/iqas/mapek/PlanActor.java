package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.kafka.KafkaTopicMsg;
import fr.isae.iqas.kafka.RequestMapping;
import fr.isae.iqas.kafka.TopicEntity;
import fr.isae.iqas.model.jsonld.VirtualSensor;
import fr.isae.iqas.model.jsonld.VirtualSensorList;
import fr.isae.iqas.model.message.PipelineRequestMsg;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.quality.MySpecificQoOAttributeComputation;
import fr.isae.iqas.model.request.State;
import fr.isae.iqas.pipelines.IPipeline;
import fr.isae.iqas.pipelines.Pipeline;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static fr.isae.iqas.model.message.MAPEKInternalMsg.*;

/**
 * Created by an.auger on 13/09/2016.
 */
public class PlanActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Properties prop;
    private MongoController mongoController;
    private FusekiController fusekiController;

    private ActorRef kafkaAdminActor;
    private ActorRef monitorActor;
    private Map<String, List<String>> actorPathRefs; // Requests <-> [ActorPathNames]
    private Map<String, ActorRef> execActorsRefs; // ActorPathNames <-> ActorRefs

    public PlanActor(Properties prop, MongoController mongoController, FusekiController fusekiController, ActorRef kafkaAdminActor) {
        this.prop = prop;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;
        this.kafkaAdminActor = kafkaAdminActor;

        this.monitorActor = getContext().actorFor(getContext().parent().path().child("monitorActor"));
        this.actorPathRefs = new ConcurrentHashMap<>();
        this.execActorsRefs = new ConcurrentHashMap<>();
    }

    @Override
    public void preStart() {
    }

    @Override
    public void onReceive(Object message) throws Exception {
        /**
         * RFCs messages
         */
        if (message instanceof RFCMsg) {
            log.info("Received RFC message: {}", message);
            RFCMsg rfcMsg = (RFCMsg) message;

            if (rfcMsg.getRfc() == RFCMAPEK.CREATE && rfcMsg.getAbout() == EntityMAPEK.REQUEST) {
                RequestMapping requestMapping = rfcMsg.getRequestMapping();

                // Creation of all topics
                requestMapping.getAllTopics().forEach((key, value) -> {
                    if (!value.isSource()) {
                        ActionMsg action = new ActionMsg(ActionMAPEK.CREATE,
                                EntityMAPEK.KAFKA_TOPIC,
                                String.valueOf(key));
                        performAction(action);
                    }
                });

                if (requestMapping.getConstructedFromRequest().equals("")) { // The request does not depend on any existing request
                    // Primary sources and filtering by sensors
                    String pipelineID = "";
                    String tempID = "";
                    Set<String> topicBaseToPullFrom = new HashSet<>();
                    TopicEntity nextTopic = null;
                    for (TopicEntity t : requestMapping.getPrimarySources()) {
                        topicBaseToPullFrom.add(t.getName());
                        if (nextTopic == null) {
                            nextTopic = requestMapping.getAllTopics().get(t.getChildren().get(0));
                            if (pipelineID.equals("")) {
                                pipelineID = t.getEnforcedPipelines().get(nextTopic.getName()).split("_")[0];
                                tempID = t.getEnforcedPipelines().get(nextTopic.getName()).split("_")[1];
                            }
                        }
                    }

                    String finalNextTopicName = nextTopic.getName();
                    String finalTempID = tempID;
                    retrievePipeline(pipelineID).whenComplete((pipeline, throwable) -> {
                        if (throwable != null) {
                            log.error(throwable.toString());
                        }
                        else {
                            if (pipeline.getPipelineID().equals("SensorFilterPipeline")) {
                                VirtualSensorList vList = fusekiController._findAllSensorsWithConditions(rfcMsg.getRequest().getLocation(), rfcMsg.getRequest().getTopic());
                                String sensorsToKeep = "";
                                for (VirtualSensor v : vList.sensors) {
                                    sensorsToKeep += v.sensor_id.split("#")[1] + ";";
                                }
                                sensorsToKeep = sensorsToKeep.substring(0, sensorsToKeep.length() - 1);
                                pipeline.setCustomizableParameter("allowed_sensors", sensorsToKeep);
                            }

                            pipeline.setAssociatedRequestID(rfcMsg.getAssociatedRequest_id());
                            pipeline.setTempID(finalTempID);

                            //TODO resolve dynamically
                            pipeline.setOptionsForMAPEKReporting(monitorActor, new FiniteDuration(10, TimeUnit.SECONDS));
                            Map<String, Map<String, String>> qooParamsForAllTopics = new HashMap<>();
                            qooParamsForAllTopics.put("sensor01", new HashMap<>());
                            qooParamsForAllTopics.put("sensor02", new HashMap<>());
                            qooParamsForAllTopics.get("sensor01").put("min_value", "-50.0");
                            qooParamsForAllTopics.get("sensor01").put("max_value", "+50.0");
                            qooParamsForAllTopics.get("sensor02").put("min_value", "0.0");
                            qooParamsForAllTopics.get("sensor02").put("max_value", "+50.0");
                            pipeline.setOptionsForQoOComputation(new MySpecificQoOAttributeComputation(), qooParamsForAllTopics);

                            pipeline.setCustomizableParameter("threshold_min", "0.0");

                            ActionMsg action = new ActionMsg(ActionMAPEK.APPLY,
                                    EntityMAPEK.PIPELINE,
                                    pipeline,
                                    topicBaseToPullFrom,
                                    finalNextTopicName,
                                    rfcMsg.getAssociatedRequest_id());

                            performAction(action);
                        }
                    });

                    // Building the rest of the graph
                    String currTopicName = finalNextTopicName;

                    while (requestMapping.getAllTopics().get(currTopicName).getChildren().size() > 0) {
                        TopicEntity currTopic = requestMapping.getAllTopics().get(currTopicName);
                        Set<String> currTopicSet = new HashSet<>();
                        currTopicSet.add(currTopic.getName());
                        TopicEntity childrenTopic = requestMapping.getAllTopics().get(currTopic.getChildren().get(0));

                        final String pipelineIDInt = currTopic.getEnforcedPipelines().get(childrenTopic.getName()).split("_")[0];
                        final String tempIDInt = currTopic.getEnforcedPipelines().get(childrenTopic.getName()).split("_")[1];
                        retrievePipeline(pipelineIDInt).whenComplete((pipeline, throwable) -> {
                            if (throwable != null) {
                                log.error(throwable.toString());
                            }
                            else {
                                pipeline.setAssociatedRequestID(rfcMsg.getAssociatedRequest_id());
                                pipeline.setTempID(tempIDInt);

                                //TODO resolve dynamically
                                pipeline.setOptionsForMAPEKReporting(monitorActor, new FiniteDuration(10, TimeUnit.SECONDS));
                                Map<String, Map<String, String>> qooParamsForAllTopics = new HashMap<>();
                                qooParamsForAllTopics.put("sensor01", new HashMap<>());
                                qooParamsForAllTopics.put("sensor02", new HashMap<>());
                                qooParamsForAllTopics.get("sensor01").put("min_value", "-50.0");
                                qooParamsForAllTopics.get("sensor01").put("max_value", "+50.0");
                                qooParamsForAllTopics.get("sensor02").put("min_value", "0.0");
                                qooParamsForAllTopics.get("sensor02").put("max_value", "+50.0");
                                pipeline.setOptionsForQoOComputation(new MySpecificQoOAttributeComputation(), qooParamsForAllTopics);

                                pipeline.setCustomizableParameter("threshold_min", "0.0");

                                ActionMsg action = new ActionMsg(ActionMAPEK.APPLY,
                                        EntityMAPEK.PIPELINE,
                                        pipeline,
                                        currTopicSet,
                                        childrenTopic.getName(),
                                        rfcMsg.getAssociatedRequest_id());

                                performAction(action);
                            }
                        });
                        currTopicName = childrenTopic.getName();
                    }
                }
                else { // The request depends on another request
                    TopicEntity beforeLastTopic = requestMapping.getPrimarySources().get(0);
                    Set<String> currTopicSet = new HashSet<>();
                    currTopicSet.add(beforeLastTopic.getName());
                    TopicEntity childrenTopic = requestMapping.getFinalSink();

                    retrievePipeline("ForwardPipeline").whenComplete((pipeline, throwable) -> {
                        if (throwable != null) {
                            log.error(throwable.toString());
                        }
                        else {
                            ActionMsg action = new ActionMsg(ActionMAPEK.APPLY,
                                    EntityMAPEK.PIPELINE,
                                    pipeline,
                                    currTopicSet,
                                    childrenTopic.getName(),
                                    rfcMsg.getAssociatedRequest_id());

                            performAction(action);
                        }
                    });
                }

                rfcMsg.getRequest().updateState(State.Status.ENFORCED);
                mongoController.updateRequest(rfcMsg.getRequest().getRequest_id(), rfcMsg.getRequest()).whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("Update of the Request " + rfcMsg.getRequest().getRequest_id() + " has failed");
                    }
                });
            }
        }
        /**
         * TerminatedMsg messages
         */
        else if (message instanceof TerminatedMsg) {
            TerminatedMsg terminatedMsg = (TerminatedMsg) message;
            ActorRef actorRefToStop = terminatedMsg.getTargetToStop();
            if (terminatedMsg.getTargetToStop().path().equals(getSelf().path())) {
                log.info("Received TerminatedMsg message: {}", message);
                getContext().stop(self());
            }
            else {
                gracefulStop(actorRefToStop);
                // TODO
                //execActorsRefs.remove(actorRefToStop);
            }
        }
    }

    private void gracefulStop(ActorRef actorRefToStop) {
        log.info("Trying to stop " + actorRefToStop.path().name());
        try {
            Future<Boolean> stopped = Patterns.gracefulStop(actorRefToStop, Duration.create(5, TimeUnit.SECONDS), new TerminatedMsg(actorRefToStop));
            Await.result(stopped, Duration.create(5, TimeUnit.SECONDS));
            log.info("Successfully stopped " + actorRefToStop.path());
        } catch (Exception e) {
            log.error(e.toString());
        }
    }

    private CompletableFuture<IPipeline> retrievePipeline(String pipeline_id) {
        CompletableFuture<IPipeline> pipelineCompletableFuture = new CompletableFuture<>();
        getPipelineWatcherActor().onComplete(new OnComplete<ActorRef>() {
            @Override
            public void onComplete(Throwable t, ActorRef pipelineWatcherActor) throws Throwable {
                if (t != null) {
                    log.error("Unable to find the PipelineWatcherActor: " + t.toString());
                } else {
                    Patterns.ask(pipelineWatcherActor, new PipelineRequestMsg(pipeline_id), new Timeout(5, TimeUnit.SECONDS)).onComplete(new OnComplete<Object>() {
                        @Override
                        public void onComplete(Throwable t, Object pipelineObject) throws Throwable {
                            if (t != null) {
                                log.error("Unable to find the PipelineWatcherActor: " + t.toString());
                            } else {
                                ArrayList<Pipeline> castedResultPipelineObject = (ArrayList<Pipeline>) pipelineObject;
                                if (castedResultPipelineObject.size() == 0) {
                                    log.error("Unable to retrieve the specified QoO pipeline " + pipeline_id);
                                } else {
                                    IPipeline pipelineToEnforce = castedResultPipelineObject.get(0).getPipelineObject();
                                    pipelineCompletableFuture.complete(pipelineToEnforce);
                                }
                            }
                        }
                    }, getContext().dispatcher());
                }
            }
        }, getContext().dispatcher());

        return pipelineCompletableFuture;
    }

    private boolean performAction(ActionMsg actionMAPEK) {
        log.info("Processing ActionMsg: {} {}", actionMAPEK.getAction(), actionMAPEK.getAbout());

        if (actionMAPEK.getAction() == ActionMAPEK.APPLY && actionMAPEK.getAbout() == EntityMAPEK.PIPELINE) {
            /*if (execActorsRefs.containsKey(actorNameToResolve)) { // if reference found, the corresponding actor has been started
                ActorRef actorRefToStop = execActorsRefs.get(actorNameToResolve);
                gracefulStop(actorRefToStop);

                ActorRef actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class,
                        prop,
                        actionMAPEK.getAttachedObject1(),
                        actionMAPEK.getAttachedObject2(),
                        actionMAPEK.getAttachedObject3(),
                        actionMAPEK.getAssociatedRequest_id()));
                execActorsRefs.put(actorNameToResolve, actorRefToStart);
                log.info("Successfully started " + actorRefToStart.path());
            } else {*/
                ActorRef actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class,
                        prop,
                        actionMAPEK.getPipelineToEnforce(),
                        actionMAPEK.getTopicsToPullFrom(),
                        actionMAPEK.getTopicToPublish()));

            actorPathRefs.computeIfAbsent(actionMAPEK.getAssociatedRequest_id(), k -> new ArrayList<>());
            actorPathRefs.get(actionMAPEK.getAssociatedRequest_id()).add(actorRefToStart.path().name());
            execActorsRefs.put(actorRefToStart.path().name(), actorRefToStart);

            log.info("Successfully started " + actorRefToStart.path());

            // TODO
            /*System.err.println(actorPathRefs.toString());
            System.err.println(execActorsRefs.toString());*/
            //}
        }
        else if (actionMAPEK.getAction() == ActionMAPEK.CREATE && actionMAPEK.getAbout() == EntityMAPEK.KAFKA_TOPIC) {
            Timeout timeout = new Timeout(Duration.create(5, "seconds"));
            Future<Object> future = Patterns.ask(kafkaAdminActor, new KafkaTopicMsg(KafkaTopicMsg.TopicAction.CREATE, actionMAPEK.getKafkaTopicID()), timeout);
            try {
                return (boolean) (Boolean) Await.result(future, timeout.duration());
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }

        }

        return true;
    }

    private Future<ActorRef> getPipelineWatcherActor() {
        return getContext().actorSelection(getSelf().path().parent().parent().parent()
                + "/" + "pipelineWatcherActor").resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }
}
