package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;
import fr.isae.iqas.config.Config;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.kafka.KafkaTopicMsg;
import fr.isae.iqas.kafka.RequestMapping;
import fr.isae.iqas.kafka.TopicEntity;
import fr.isae.iqas.model.jsonld.SensorCapability;
import fr.isae.iqas.model.jsonld.SensorCapabilityList;
import fr.isae.iqas.model.jsonld.VirtualSensorList;
import fr.isae.iqas.model.message.PipelineRequestMsg;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.quality.MySpecificQoOAttributeComputation;
import fr.isae.iqas.model.request.HealRequest;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.model.request.State;
import fr.isae.iqas.pipelines.*;
import fr.isae.iqas.utils.ActorUtils;
import fr.isae.iqas.utils.JenaUtils;
import fr.isae.iqas.utils.MapUtils;
import org.apache.jena.ontology.OntModel;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static akka.dispatch.Futures.future;
import static fr.isae.iqas.model.message.MAPEKInternalMsg.*;
import static fr.isae.iqas.utils.PipelineUtils.*;

/**
 * Created by an.auger on 13/09/2016.
 */
public class PlanActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Config iqasConfig;
    private Properties prop;
    private MongoController mongoController;
    private FusekiController fusekiController;

    private FiniteDuration reportIntervalRateAndQoO;
    private VirtualSensorList virtualSensorList;
    private OntModel qooBaseModel;

    private ActorRef kafkaAdminActor;
    private ActorRef monitorActor;

    private Map<String, List<String>> actorPathRefs; // Requests <-> [ActorPathNames]
    private Map<String, Integer> execActorsCount; // ActorPathNames <-> # uses
    private Map<String, ActorRef> execActorsRefs; // ActorPathNames <-> ActorRefs
    private Map<String, IPipeline> enforcedPipelines; // ActorPathNames <-> IPipeline objects
    private Map<String, String> mappingTopicsActors; // DestinationTopic <-> ActorPathNames

    public PlanActor(Config iqasConfig, MongoController mongoController, FusekiController fusekiController, ActorRef kafkaAdminActor) {
        this.iqasConfig = iqasConfig;
        this.prop = iqasConfig.getProp();
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;
        this.kafkaAdminActor = kafkaAdminActor;

        this.reportIntervalRateAndQoO = new FiniteDuration(Long.valueOf(prop.getProperty("report_interval_seconds")), TimeUnit.SECONDS);

        this.monitorActor = getContext().actorFor(getContext().parent().path().child("monitorActor"));
        this.actorPathRefs = new ConcurrentHashMap<>();
        this.execActorsRefs = new ConcurrentHashMap<>();
        this.execActorsCount = new ConcurrentHashMap<>();
        this.mappingTopicsActors = new ConcurrentHashMap<>();
        this.enforcedPipelines = new ConcurrentHashMap<>();
    }

    @Override
    public void preStart() {
        future(() -> fusekiController.findAllSensors(), context().dispatcher())
                .onComplete(new OnComplete<VirtualSensorList>() {
                    public void onComplete(Throwable throwable, VirtualSensorList virtualSensorListResult) {
                        if (throwable == null) { // Only continue if there was no error so far
                            virtualSensorList = virtualSensorListResult;
                        }
                    }
                }, context().dispatcher());

        future(() -> JenaUtils.loadQoOntoWithImports(iqasConfig), context().dispatcher())
                .onComplete(new OnComplete<OntModel>() {
                    public void onComplete(Throwable throwable, OntModel qooBaseModelResult) {
                        if (throwable == null) { // Only continue if there was no error so far
                            qooBaseModel = qooBaseModelResult;
                        }
                    }
                }, context().dispatcher());
    }

    @Override
    public void onReceive(Object message) throws Exception {
        // RFCs messages
        if (message instanceof RFCMsg) {
            log.info("Received RFC message: {}", message);
            RFCMsg rfcMsg = (RFCMsg) message;

            // RFCs messages - Request CREATE
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

                future(() -> fusekiController.findAllSensorCapabilities(), context().dispatcher())
                        .onComplete(new OnComplete<SensorCapabilityList>() {
                            public void onComplete(Throwable throwable, SensorCapabilityList sensorCapabilityList) {
                                if (throwable != null) {
                                    log.error("Error when retrieving sensor capabilities. " + throwable.toString());
                                }
                                else {
                                    Map<String, Map<String, String>> qooParamsForAllTopics = new ConcurrentHashMap<>();
                                    for (SensorCapability s : sensorCapabilityList.sensorCapabilities) {
                                        qooParamsForAllTopics.putIfAbsent(s.sensor_id, new ConcurrentHashMap<>());
                                        qooParamsForAllTopics.get(s.sensor_id).put("min_value", s.min_value);
                                        qooParamsForAllTopics.get(s.sensor_id).put("max_value", s.max_value);
                                    }

                                    // The incoming request has no common points with the enforced ones
                                    if (requestMapping.getConstructedFromRequest().equals("")) {
                                        buildGraphForFirstTime(requestMapping, incomingRequest, qooParamsForAllTopics);
                                    }
                                    else { // The incoming request may reuse existing enforced requests
                                        TopicEntity beforeLastTopic = requestMapping.getPrimarySources().get(0);
                                        Set<String> currTopicSet = new HashSet<>();
                                        currTopicSet.add(beforeLastTopic.getName());
                                        final int finalmaxLevelDepth = beforeLastTopic.getLevel();
                                        TopicEntity finalSinkForApp = requestMapping.getFinalSink();

                                        retrievePipeline("OutputPipeline").whenComplete((pipeline, throwable2) -> {
                                            if (throwable2 != null) {
                                                log.error(throwable2.toString());
                                            }
                                            else {
                                                // Setting request_id, temp_id and other options for the retrieved Pipeline
                                                pipeline.setAssociatedRequestID(incomingRequest.getRequest_id());
                                                pipeline.setTempID(requestMapping.getPrimarySources().get(0).getEnforcedPipelines()
                                                        .get(requestMapping.getFinalSink().getName()).split("_")[1]);
                                                pipeline.setOptionsForMAPEKReporting(monitorActor, reportIntervalRateAndQoO);
                                                pipeline.setOptionsForQoOComputation(new MySpecificQoOAttributeComputation(), qooParamsForAllTopics);

                                                // Specific settings since it is an OutputPipeline
                                                setOptionsForOutputPipeline((OutputPipeline) pipeline, iqasConfig, incomingRequest, virtualSensorList, qooBaseModel);

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
                                }
                            }
                        }, context().dispatcher());

                incomingRequest.addLog("Successfully created pipeline graph by enforcing following qoo constraints: "
                        + incomingRequest.getQooConstraints().getIqas_params().toString());
                incomingRequest.updateState(State.Status.ENFORCED);
                mongoController.updateRequest(incomingRequest.getRequest_id(), incomingRequest).whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("Update of the Request " + incomingRequest.getRequest_id() + " has failed");
                    }
                });
            }

            // RFCs messages - Request REMOVE
            else if (rfcMsg.getRfc() == RFCMAPEK.REMOVE && rfcMsg.getAbout() == EntityMAPEK.REQUEST) { // Request deleted by the user
                Request requestToDelete = rfcMsg.getRequest();
                deleteRequest(requestToDelete);
            }

            // RFCs messages - Sensor UPDATE
            else if (rfcMsg.getRfc() == RFCMAPEK.UPDATE && rfcMsg.getAbout() == EntityMAPEK.SENSOR) { // Sensor description has been updated on Fuseki
                future(() -> fusekiController.findAllSensors(), context().dispatcher())
                        .onComplete(new OnComplete<VirtualSensorList>() {
                            public void onComplete(Throwable throwable, VirtualSensorList newVirtualSensorList) {
                                if (throwable == null) { // Only continue if there was no error so far
                                    virtualSensorList = newVirtualSensorList;
                                    enforcedPipelines.forEach((actorPathName, pipelineObject) -> {
                                        if (pipelineObject instanceof OutputPipeline) {
                                            ((OutputPipeline) pipelineObject).setSensorContext(iqasConfig, virtualSensorList, qooBaseModel);
                                            execActorsRefs.get(actorPathName).tell(pipelineObject, getSelf());
                                        }
                                    });
                                }
                            }
                        }, context().dispatcher());
            }

            // RFCs messages - Request HEAL
            else if (rfcMsg.getRfc() == RFCMAPEK.HEAL && rfcMsg.getAbout() == EntityMAPEK.REQUEST) {

            }

            // RFCs messages - Request RESET
            else if (rfcMsg.getRfc() == RFCMAPEK.RESET && rfcMsg.getAbout() == EntityMAPEK.REQUEST) {

            }
        }
        // TerminatedMsg messages
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
        ActorUtils.getPipelineWatcherActor(getContext(), self()).onComplete(new OnComplete<ActorRef>() {
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

    private void buildGraphForFirstTime(RequestMapping requestMapping, Request incomingRequest, Map<String,Map<String,String>> qooParamsForAllTopics) {
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
        retrievePipeline(pipelineID).whenComplete((pipeline, throwable2) -> {
            if (throwable2 != null) {
                log.error(throwable2.toString());
            }
            else {
                if (pipeline instanceof IngestPipeline) {
                    setOptionsForIngestPipeline((IngestPipeline) pipeline, incomingRequest, fusekiController, context());
                }

                // Setting request_id, temp_id and other options for the retrieved Pipeline
                pipeline.setAssociatedRequestID(incomingRequest.getRequest_id());
                pipeline.setTempID(finalTempID);
                pipeline.setOptionsForMAPEKReporting(monitorActor, reportIntervalRateAndQoO);
                pipeline.setOptionsForQoOComputation(new MySpecificQoOAttributeComputation(), qooParamsForAllTopics);

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
            retrievePipeline(pipelineIDInt).whenComplete((pipeline, throwable3) -> {
                if (throwable3 != null) {
                    log.error(throwable3.toString());
                }
                else {
                    if (pipeline instanceof FilterPipeline) {
                        setOptionsForFilterPipeline((FilterPipeline) pipeline, incomingRequest);
                    }
                    else if (pipeline instanceof ThrottlePipeline) {
                        setOptionsForThrottlePipeline((ThrottlePipeline) pipeline, incomingRequest);
                    }
                    else if (pipeline instanceof OutputPipeline) {
                        setOptionsForOutputPipeline((OutputPipeline) pipeline, iqasConfig, incomingRequest, virtualSensorList, qooBaseModel);
                        ((OutputPipeline) pipeline).setSensorContext(iqasConfig, virtualSensorList, qooBaseModel);
                    }

                    pipeline.setAssociatedRequestID(incomingRequest.getRequest_id());
                    pipeline.setTempID(tempIDInt);
                    pipeline.setOptionsForMAPEKReporting(monitorActor, reportIntervalRateAndQoO);
                    pipeline.setOptionsForQoOComputation(new MySpecificQoOAttributeComputation(), qooParamsForAllTopics);

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

    private void deleteRequest(Request requestToDelete) {
        Set<ActorRef> actorsToShutDown = new HashSet<>();
        Set<String> kafkaTopicsToDelete = new HashSet<>();
        for (String s : actorPathRefs.get(requestToDelete.getRequest_id())) {
            execActorsCount.put(s, execActorsCount.get(s) - 1);
            if (execActorsCount.get(s) == 0) { // this means that resources can be released
                String topicTemp = MapUtils.getKeyByValue(mappingTopicsActors, s);
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

        // For cleaning up resources in Monitor actor
        monitorActor.tell(new SymptomMsg(SymptomMAPEK.REMOVED, EntityMAPEK.REQUEST, requestToDelete), getSelf());

        // Removal of the corresponding RequestMapping
        actorPathRefs.remove(requestToDelete.getRequest_id());
        mongoController.deleteRequestMapping(requestToDelete.getRequest_id());
    }

    private boolean performAction(ActionMsg actionMsg) {
        log.info("Processing ActionMsg: {} {}", actionMsg.getAction(), actionMsg.getAbout());

        if (actionMsg.getAction() == ActionMAPEK.APPLY && actionMsg.getAbout() == EntityMAPEK.PIPELINE) {
            ActorRef actorRefToStart;
            if (!mappingTopicsActors.containsKey(actionMsg.getTopicToPublish())) {
                if (actionMsg.getPipelineToEnforce() instanceof IngestPipeline) {
                    actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class,
                            prop,
                            actionMsg.getPipelineToEnforce(),
                            actionMsg.getAskedObsLevel(),
                            actionMsg.getTopicsToPullFrom(),
                            actionMsg.getTopicToPublish())
                            .withDispatcher("dispatchers.pipelines.ingest"));
                }
                else {
                    actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class,
                            prop,
                            actionMsg.getPipelineToEnforce(),
                            actionMsg.getAskedObsLevel(),
                            actionMsg.getTopicsToPullFrom(),
                            actionMsg.getTopicToPublish()));
                }

                enforcedPipelines.put(actorRefToStart.path().name(), actionMsg.getPipelineToEnforce());
                mappingTopicsActors.put(actionMsg.getTopicToPublish(), actorRefToStart.path().name());
                execActorsRefs.put(actorRefToStart.path().name(), actorRefToStart);
            }
            else {
                actorRefToStart = execActorsRefs.get(mappingTopicsActors.get(actionMsg.getTopicToPublish()));
            }

            actorPathRefs.computeIfAbsent(actionMsg.getAssociatedRequest_id(), k -> new ArrayList<>());
            actorPathRefs.get(actionMsg.getAssociatedRequest_id()).add(actorRefToStart.path().name());
            execActorsCount.putIfAbsent(actorRefToStart.path().name(), 0);
            execActorsCount.put(actorRefToStart.path().name(), execActorsCount.get(actorRefToStart.path().name()) + 1);

            if (actionMsg.getPipelineToEnforce() instanceof IngestPipeline) {
                monitorActor.tell(new SymptomMsg(SymptomMAPEK.NEW, EntityMAPEK.PIPELINE, enforcedPipelines.get(actorRefToStart.path().name()).getUniqueID(), actionMsg.getAssociatedRequest_id()), getSelf());
            }

            if (!actionMsg.getConstructedFromRequest().equals("")) { // Additional steps to perform if the request depends on another one
                mongoController.getSpecificRequestMapping(actionMsg.getConstructedFromRequest()).whenComplete((ancestorReqMapping, throwable) -> {
                    if (throwable == null) {
                        for (Object o : ancestorReqMapping.getAllTopics().entrySet()) {
                            Map.Entry pair = (Map.Entry) o;
                            TopicEntity topicEntityTemp = (TopicEntity) pair.getValue();
                            if (topicEntityTemp.getLevel() < actionMsg.getMaxLevelDepth() + 1) {
                                if (mappingTopicsActors.containsKey(topicEntityTemp.getName())) {
                                    ActorRef tempActor = execActorsRefs.get(mappingTopicsActors.get(topicEntityTemp.getName()));
                                    actorPathRefs.get(actionMsg.getAssociatedRequest_id()).add(tempActor.path().name());
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
        else if (actionMsg.getAction() == ActionMAPEK.CREATE && actionMsg.getAbout() == EntityMAPEK.KAFKA_TOPIC) {
            Timeout timeout = new Timeout(Duration.create(5, TimeUnit.SECONDS));
            Future<Object> future = Patterns.ask(kafkaAdminActor, new KafkaTopicMsg(KafkaTopicMsg.TopicAction.CREATE, actionMsg.getKafkaTopicID()), timeout);
            try {
                return (boolean) (Boolean) Await.result(future, timeout.duration());
            } catch (Exception e) {
                log.error(e.toString());
                return false;
            }
        }
        else if (actionMsg.getAction() == ActionMAPEK.DELETE && actionMsg.getAbout() == EntityMAPEK.KAFKA_TOPIC) {
            Timeout timeout = new Timeout(Duration.create(5, TimeUnit.SECONDS));
            Future<Object> future = Patterns.ask(kafkaAdminActor, new KafkaTopicMsg(KafkaTopicMsg.TopicAction.DELETE, actionMsg.getKafkaTopicID()), timeout);
            try {
                return (boolean) (Boolean) Await.result(future, timeout.duration());
            } catch (Exception e) {
                log.error(e.toString());
                return false;
            }
        }

        return true;
    }

    // TODO ActionMsg for that?

    private void healRequest(RequestMapping currEnforcedRM, HealRequest request) {
        //request.getConcernedRequest()
    }

    private void resetRequest(RequestMapping currEnforcedRM, HealRequest request) {

    }

}
