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
import fr.isae.iqas.model.request.Request;
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
    private Map<String, Integer> execActorsCount; // ActorPathNames <-> # uses
    private Map<String, ActorRef> execActorsRefs; // ActorPathNames <-> ActorRefs
    private Map<String, IPipeline> enforcedPipelines; // ActorPathNames <-> IPipeline objects
    private Map<String, String> mappingTopicsActors; // DestinationTopic <-> ActorPathNames

    public PlanActor(Properties prop, MongoController mongoController, FusekiController fusekiController, ActorRef kafkaAdminActor) {
        this.prop = prop;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;
        this.kafkaAdminActor = kafkaAdminActor;

        this.monitorActor = getContext().actorFor(getContext().parent().path().child("monitorActor"));
        this.actorPathRefs = new ConcurrentHashMap<>();
        this.execActorsRefs = new ConcurrentHashMap<>();
        this.execActorsCount = new ConcurrentHashMap<>();
        this.mappingTopicsActors = new ConcurrentHashMap<>();
        this.enforcedPipelines = new ConcurrentHashMap<>();
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
                Request incomingRequest = rfcMsg.getRequest();
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

                if (requestMapping.getConstructedFromRequest().equals("")) { // The incoming request has no common points with the enforced ones
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
                                VirtualSensorList vList = fusekiController._findAllSensorsWithConditions(incomingRequest.getLocation(), incomingRequest.getTopic());
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
                                    incomingRequest.getObs_level(),
                                    topicBaseToPullFrom,
                                    finalNextTopicName,
                                    requestMapping.getRequest_id(),
                                    requestMapping.getConstructedFromRequest(),
                                    -1);

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
                                        incomingRequest.getObs_level(),
                                        currTopicSet,
                                        childrenTopic.getName(),
                                        requestMapping.getRequest_id(),
                                        requestMapping.getConstructedFromRequest(),
                                        -1);

                                performAction(action);
                            }
                        });
                        currTopicName = childrenTopic.getName();
                    }
                }
                else { // The incoming request may reuse existing enforced requests
                    TopicEntity beforeLastTopic = requestMapping.getPrimarySources().get(0);
                    Set<String> currTopicSet = new HashSet<>();
                    currTopicSet.add(beforeLastTopic.getName());
                    final int finalmaxLevelDepth = beforeLastTopic.getLevel();
                    TopicEntity finalSinkForApp = requestMapping.getFinalSink();

                    retrievePipeline("ForwardPipeline").whenComplete((pipeline, throwable) -> {
                        if (throwable != null) {
                            log.error(throwable.toString());
                        }
                        else {
                            ActionMsg action = new ActionMsg(ActionMAPEK.APPLY,
                                    EntityMAPEK.PIPELINE,
                                    pipeline,
                                    incomingRequest.getObs_level(),
                                    currTopicSet,
                                    finalSinkForApp.getName(),
                                    requestMapping.getRequest_id(),
                                    requestMapping.getConstructedFromRequest(),
                                    finalmaxLevelDepth);

                            performAction(action);
                        }
                    });
                }

                incomingRequest.addLog("Successfully created pipeline graph without any QoO constraints.");
                incomingRequest.updateState(State.Status.ENFORCED);
                mongoController.updateRequest(incomingRequest.getRequest_id(), incomingRequest).whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("Update of the Request " + incomingRequest.getRequest_id() + " has failed");
                    }
                });
            }
            else if (rfcMsg.getRfc() == RFCMAPEK.REMOVE && rfcMsg.getAbout() == EntityMAPEK.REQUEST) { // Request deleted by the user
                Request requestToDelete = rfcMsg.getRequest();

                Set<ActorRef> actorsToShutDown = new HashSet<>();
                Set<String> kafkaTopicsToDelete = new HashSet<>();
                for (String s : actorPathRefs.get(requestToDelete.getRequest_id())) {
                    execActorsCount.put(s, execActorsCount.get(s) - 1);
                    if (execActorsCount.get(s) == 0) { // this means that resources can be released
                        String topicTemp = getKeyByValue(mappingTopicsActors, s);
                        if (topicTemp != null) {
                            kafkaTopicsToDelete.add(topicTemp);
                        }
                        actorsToShutDown.add(execActorsRefs.get(s));
                    }
                }

                // We stop the workers that enforce the different pipelines
                for (ActorRef actorRef : actorsToShutDown) {
                    gracefulStop(actorRef);
                    execActorsCount.remove(actorRef.path().name());
                    execActorsRefs.remove(actorRef.path().name());
                    monitorActor.tell(new SymptomMsg(SymptomMAPEK.REMOVED, EntityMAPEK.PIPELINE, enforcedPipelines.get(actorRef.path().name()).getUniqueID()), getSelf());
                    enforcedPipelines.remove(actorRef.path().name());
                }

                // We clean up the unused topics
                for (String topic : kafkaTopicsToDelete) {
                    performAction(new ActionMsg(ActionMAPEK.DELETE, EntityMAPEK.KAFKA_TOPIC, topic));
                    log.error("Removing: " + topic);
                    mappingTopicsActors.remove(topic);
                }

                // Removal of the corresponding RequestMapping
                actorPathRefs.remove(requestToDelete.getRequest_id());
                mongoController.deleteRequestMapping(requestToDelete.getRequest_id());
                /*.whenComplete((r, t) -> {
                    log.error(mappingTopicsActors.toString());
                    log.error(actorPathRefs.toString());
                    log.error(execActorsCount.toString());
                    log.error(execActorsRefs.toString());
                });*/
            }
        }
        /**
         * TerminatedMsg messages
         */
        else if (message instanceof TerminatedMsg) {
            TerminatedMsg terminatedMsg = (TerminatedMsg) message;
            if (terminatedMsg.getTargetToStop().path().equals(getSelf().path())) {
                log.info("Received TerminatedMsg message: {}", message);
                getContext().stop(self());
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

            ActorRef actorRefToStart;
            if (!mappingTopicsActors.containsKey(actionMAPEK.getTopicToPublish())) {
                actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class,
                        prop,
                        actionMAPEK.getPipelineToEnforce(),
                        actionMAPEK.getAskedObsLevel(),
                        actionMAPEK.getTopicsToPullFrom(),
                        actionMAPEK.getTopicToPublish()));
            }
            else {
                actorRefToStart = execActorsRefs.get(mappingTopicsActors.get(actionMAPEK.getTopicToPublish()));
            }

            enforcedPipelines.put(actorRefToStart.path().name(), actionMAPEK.getPipelineToEnforce());
            mappingTopicsActors.put(actionMAPEK.getTopicToPublish(), actorRefToStart.path().name());
            actorPathRefs.computeIfAbsent(actionMAPEK.getAssociatedRequest_id(), k -> new ArrayList<>());
            actorPathRefs.get(actionMAPEK.getAssociatedRequest_id()).add(actorRefToStart.path().name());
            execActorsRefs.put(actorRefToStart.path().name(), actorRefToStart);
            execActorsCount.computeIfAbsent(actorRefToStart.path().name(), k -> 0);
            execActorsCount.put(actorRefToStart.path().name(), execActorsCount.get(actorRefToStart.path().name()) + 1);

            if (!actionMAPEK.getConstructedFromRequest().equals("")) { // Additional steps to perform if the request depends on another one
                mongoController.getSpecificRequestMapping(actionMAPEK.getConstructedFromRequest()).whenComplete((result, throwable) -> {
                    if (throwable == null) {
                        RequestMapping ancestorReqMapping = result.get(0);
                        for (Object o : ancestorReqMapping.getAllTopics().entrySet()) {
                            Map.Entry pair = (Map.Entry) o;
                            TopicEntity topicEntityTemp = (TopicEntity) pair.getValue();
                            if (topicEntityTemp.getLevel() < actionMAPEK.getMaxLevelDepth() + 1) {
                                if (mappingTopicsActors.containsKey(topicEntityTemp.getName())) {
                                    ActorRef tempActor = execActorsRefs.get(mappingTopicsActors.get(topicEntityTemp.getName()));
                                    actorPathRefs.get(actionMAPEK.getAssociatedRequest_id()).add(tempActor.path().name());
                                    execActorsCount.put(mappingTopicsActors.get(topicEntityTemp.getName()), execActorsCount.get(mappingTopicsActors.get(topicEntityTemp.getName())) + 1);
                                }
                            }
                        }
                    }
                    else {
                        log.error(throwable.toString());
                    }
                });
            }

            log.info("Successfully started " + actorRefToStart.path());
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
        else if (actionMAPEK.getAction() == ActionMAPEK.DELETE && actionMAPEK.getAbout() == EntityMAPEK.KAFKA_TOPIC) {
            Timeout timeout = new Timeout(Duration.create(5, "seconds"));
            Future<Object> future = Patterns.ask(kafkaAdminActor, new KafkaTopicMsg(KafkaTopicMsg.TopicAction.DELETE, actionMAPEK.getKafkaTopicID()), timeout);
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
        return getContext().actorSelection(getSelf().path().parent().parent().parent() + "/pipelineWatcherActor").resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

    private static <T, E> T getKeyByValue(Map<T, E> map, E value) {
        for (Map.Entry<T, E> entry : map.entrySet()) {
            if (Objects.equals(value, entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }
}
