package fr.isae.iqas.mapek;

import akka.Done;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.pattern.Patterns;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
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
import fr.isae.iqas.model.message.*;
import fr.isae.iqas.model.quality.MySpecificQoOAttributeComputation;
import fr.isae.iqas.model.request.HealRequest;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.model.request.State;
import fr.isae.iqas.pipelines.*;
import fr.isae.iqas.utils.JenaUtils;
import fr.isae.iqas.utils.MapUtils;
import org.apache.jena.ontology.OntModel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static akka.dispatch.Futures.future;
import static fr.isae.iqas.model.message.MAPEKenums.*;
import static fr.isae.iqas.utils.ActorUtils.getPipelineWatcherActorFromMAPEKchild;
import static fr.isae.iqas.utils.PipelineUtils.*;

/**
 * Created by an.auger on 13/09/2016.
 */
public class PlanActor extends AbstractActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Config iqasConfig;
    private Properties prop;
    private MongoController mongoController;
    private FusekiController fusekiController;

    private FiniteDuration reportIntervalRateAndQoO;
    private VirtualSensorList virtualSensorList;
    private OntModel qooBaseModel;

    private Materializer materializer;
    private ActorRef kafkaConsumer;
    private Sink<ProducerRecord<byte[], String>, CompletionStage<Done>> kafkaSink;
    private ActorRef kafkaAdminActor;
    private ActorRef monitorActor;

    private Map<String, List<String>> actorPathRefs; // Requests <-> [ActorPathNames]
    private Map<String, Integer> execActorsCount; // ActorPathNames <-> # uses
    private Map<String, ActorRef> execActorsRefs; // ActorPathNames <-> ActorRefs
    private Map<String, IPipeline> enforcedPipelines; // ActorPathNames <-> IPipeline objects
    private Map<String, String> mappingTopicsActors; // DestinationTopic <-> ActorPathNames

    public PlanActor(Config iqasConfig,
                     MongoController mongoController,
                     FusekiController fusekiController,
                     ActorRef kafkaAdminActor,
                     ActorRef kafkaConsumer,
                     KafkaProducer<byte[], String> kafkaProducer,
                     Materializer materializer) {

        this.iqasConfig = iqasConfig;
        this.prop = iqasConfig.getProp();
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;
        this.kafkaAdminActor = kafkaAdminActor;

        ProducerSettings producerSettings = ProducerSettings
                .create(getContext().system(), new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"));

        this.materializer = materializer;
        this.kafkaConsumer = kafkaConsumer;




        this.kafkaSink = Producer.plainSink(producerSettings, kafkaProducer);

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
    public Receive createReceive() {
        return receiveBuilder()
                .match(RFCMsgRequestCreate.class, this::actionsOnRFCMsgRequestCreateMsg)
                .match(RFCMsgRequestRemove.class, this::actionsOnRFCMsgRequestRemoveMsg)
                .match(RFCMsgQoOAttr.class, this::actionsOnRFCMsgQoOAttrMsg)
                .match(RFCMsg.class, this::actionsOnRFCMsg)
                .match(TerminatedMsg.class, this::stopThisActor)
                .build();
    }

    private void actionsOnRFCMsgRequestCreateMsg(RFCMsgRequestCreate msg) {
        // RFCs messages - Request CREATE
        if (msg.getRfc() == RFCMAPEK.CREATE && msg.getAbout() == EntityMAPEK.REQUEST) {
            Request incomingRequest = msg.getRequest();
            RequestMapping requestMapping = msg.getNewRequestMapping();

            // Creation of all topics
            requestMapping.getAllTopics().forEach((key, value) -> {
                if (!value.isSource()) {
                    ActionMsg action = new ActionMsgKafka(ActionMAPEK.CREATE,
                            EntityMAPEK.KAFKA_TOPIC,
                            key);

                    performAction(action);
                }
            });

            future(() -> fusekiController.findAllSensorCapabilities(), context().dispatcher())
                    .onComplete(new OnComplete<SensorCapabilityList>() {
                        public void onComplete(Throwable throwable, SensorCapabilityList sensorCapabilityList) {
                            if (throwable != null) {
                                log.error("Error when retrieving sensor capabilities. " + throwable.toString());
                            } else {
                                Map<String, Map<String, String>> qooParamsForAllTopics = new ConcurrentHashMap<>();
                                for (SensorCapability s : sensorCapabilityList.sensorCapabilities) {
                                    qooParamsForAllTopics.putIfAbsent(s.sensor_id, new ConcurrentHashMap<>());
                                    qooParamsForAllTopics.get(s.sensor_id).put("min_value", s.min_value);
                                    qooParamsForAllTopics.get(s.sensor_id).put("max_value", s.max_value);
                                }

                                // The incoming request has no common points with the enforced ones
                                if (requestMapping.getConstructedFromRequest().equals("")) {
                                    buildGraphForFirstTime(requestMapping, incomingRequest, qooParamsForAllTopics);
                                } else { // The incoming request may reuse existing enforced requests
                                    TopicEntity beforeLastTopic = requestMapping.getPrimarySources().get(0);
                                    Set<String> currTopicSet = new HashSet<>();
                                    currTopicSet.add(beforeLastTopic.getName());
                                    final int finalmaxLevelDepth = beforeLastTopic.getLevel();
                                    TopicEntity finalSinkForApp = requestMapping.getFinalSink();

                                    retrievePipeline("OutputPipeline").whenComplete((pipeline, throwable2) -> {
                                        if (throwable2 != null) {
                                            log.error(throwable2.toString());
                                        } else {
                                            // Setting request_id, temp_id and other options for the retrieved Pipeline
                                            pipeline.setAssociatedRequestID(incomingRequest.getRequest_id());
                                            pipeline.setTempID(requestMapping.getPrimarySources().get(0).getEnforcedPipelines()
                                                    .get(requestMapping.getFinalSink().getName()).split("_")[1]);

                                            pipeline.setOptionsForMAPEKReporting(monitorActor, reportIntervalRateAndQoO);
                                            pipeline.setOptionsForQoOComputation(new MySpecificQoOAttributeComputation(), qooParamsForAllTopics);

                                            // Specific settings since it is an OutputPipeline
                                            setOptionsForOutputPipeline((OutputPipeline) pipeline, iqasConfig, incomingRequest, virtualSensorList, qooBaseModel);

                                            ActionMsg action = new ActionMsgPipeline(ActionMAPEK.APPLY,
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
    }

    private void actionsOnRFCMsgRequestRemoveMsg(RFCMsgRequestRemove msg) {
        // RFCs messages - Request REMOVE
        if (msg.getRfc() == RFCMAPEK.REMOVE && msg.getAbout() == EntityMAPEK.REQUEST) { // Request deleted by the user
            Request requestToDelete = msg.getRequest();
            deleteRequest(requestToDelete);
        }
    }

    private void actionsOnRFCMsgQoOAttrMsg(RFCMsgQoOAttr msg) {
        // RFCs messages - Request HEAL
        if (msg.getRfc() == RFCMAPEK.HEAL && msg.getAbout() == EntityMAPEK.REQUEST) {
            if (actorPathRefs.containsKey(msg.getHealRequest().getConcernedRequest())) {
                healRequest(msg.getHealRequest(), msg.getOldRequestMapping(), msg.getNewRequestMapping());
            }
        }
        // RFCs messages - Request RESET
        else if (msg.getRfc() == RFCMAPEK.RESET && msg.getAbout() == EntityMAPEK.REQUEST) {
            if (actorPathRefs.containsKey(msg.getHealRequest().getConcernedRequest())) {
                resetRequest(msg.getOldRequestMapping(), msg.getNewRequestMapping());
            }
        }
    }

    private void actionsOnRFCMsg(RFCMsg msg) {
        log.info("Received RFC message: {}", msg);
        // RFCs messages - Sensor UPDATE
        if (msg.getRfc() == RFCMAPEK.UPDATE && msg.getAbout() == EntityMAPEK.SENSOR) { // Sensor description has been updated on Fuseki
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
    }

    private void stopThisActor(TerminatedMsg msg) {
        if (msg.getTargetToStop().path().equals(getSelf().path())) {
            log.info("Received TerminatedMsg message: " + msg);
            getContext().stop(self());
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
        ActorSelection pipelineWatcherActor = getPipelineWatcherActorFromMAPEKchild(getContext(), self());

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

                ActionMsg action = new ActionMsgPipeline(ActionMAPEK.APPLY,
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

                    ActionMsg action = new ActionMsgPipeline(ActionMAPEK.APPLY,
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
            monitorActor.tell(new SymptomMsgRemovedPipeline(SymptomMAPEK.REMOVED, EntityMAPEK.PIPELINE, enforcedPipelines.get(actorRef.path().name()).getUniqueID()), getSelf());
            enforcedPipelines.remove(actorRef.path().name());
        }

        // We clean up the unused topics
        for (String topic : kafkaTopicsToDelete) {
            performAction(new ActionMsgKafka(ActionMAPEK.DELETE, EntityMAPEK.KAFKA_TOPIC, topic));
            mappingTopicsActors.remove(topic);
        }

        // For cleaning up resources in Monitor actor
        monitorActor.tell(new SymptomMsgRequest(SymptomMAPEK.REMOVED, EntityMAPEK.REQUEST, requestToDelete), getSelf());

        // Removal of the corresponding RequestMapping
        actorPathRefs.remove(requestToDelete.getRequest_id());
        mongoController.deleteRequestMapping(requestToDelete.getRequest_id());
    }

    private boolean performAction(ActionMsg actionMsgToCast) {
        log.info("Processing ActionMsg: {} {}", actionMsgToCast.getAction(), actionMsgToCast.getAbout());

        if (actionMsgToCast.getAction() == ActionMAPEK.APPLY && actionMsgToCast.getAbout() == EntityMAPEK.PIPELINE) {
            ActionMsgPipeline actionMsg = (ActionMsgPipeline) actionMsgToCast;
            ActorRef actorRefToStart;
            if (!mappingTopicsActors.containsKey(actionMsg.getTopicToPublish())) {
                if (actionMsg.getPipelineToEnforce() instanceof IngestPipeline) {


                    Set<TopicPartition> watchedTopics = new HashSet<>();
                    watchedTopics.addAll(actionMsg.getTopicsToPullFrom().stream().map(s -> new TopicPartition(s, 0)).collect(Collectors.toList()));
                    Source<ConsumerRecord<byte[], String>, Consumer.Control> kafkaSource = Consumer.plainExternalSource(kafkaConsumer, Subscriptions.assignment(watchedTopics));

                    actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class,
                            prop,
                            actionMsg.getPipelineToEnforce(),
                            actionMsg.getAskedObsLevel(),
                            kafkaSource,
                            kafkaSink,
                            materializer,
                            actionMsg.getTopicsToPullFrom(),
                            actionMsg.getTopicToPublish()));
                }
                else {
                    Set<TopicPartition> watchedTopics = new HashSet<>();
                    watchedTopics.addAll(actionMsg.getTopicsToPullFrom().stream().map(s -> new TopicPartition(s, 0)).collect(Collectors.toList()));
                    Source<ConsumerRecord<byte[], String>, Consumer.Control> kafkaSource = Consumer.plainExternalSource(kafkaConsumer, Subscriptions.assignment(watchedTopics));

                    actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class,
                            prop,
                            actionMsg.getPipelineToEnforce(),
                            actionMsg.getAskedObsLevel(),
                            kafkaSource,
                            kafkaSink,
                            materializer,
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
                monitorActor.tell(new SymptomMsgPipelineCreation(
                        SymptomMAPEK.NEW,
                        EntityMAPEK.PIPELINE,
                        enforcedPipelines.get(actorRefToStart.path().name()).getUniqueID(),
                        actionMsg.getAssociatedRequest_id()),
                        getSelf());
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
        else if (actionMsgToCast.getAction() == ActionMAPEK.CREATE && actionMsgToCast.getAbout() == EntityMAPEK.KAFKA_TOPIC) {
            ActionMsgKafka actionMsg = (ActionMsgKafka) actionMsgToCast;
            Timeout timeout = new Timeout(Duration.create(5, TimeUnit.SECONDS));
            Future<Object> future = Patterns.ask(kafkaAdminActor, new KafkaTopicMsg(KafkaTopicMsg.TopicAction.CREATE, actionMsg.getKafkaTopicID()), timeout);
            try {
                return (boolean) (Boolean) Await.result(future, timeout.duration());
            } catch (Exception e) {
                log.error(e.toString());
                return false;
            }
        }
        else if (actionMsgToCast.getAction() == ActionMAPEK.DELETE && actionMsgToCast.getAbout() == EntityMAPEK.KAFKA_TOPIC) {
            ActionMsgKafka actionMsg = (ActionMsgKafka) actionMsgToCast;
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

    private void healRequest(HealRequest healRequest, RequestMapping oldRequestMapping, RequestMapping newRequestMapping) {
        // We stop the sink and heal topics currently enforced
        IPipeline pipelineAssociatedToFinalSink = stopHealAndSinkTopics(oldRequestMapping, false);

        final String finalSink = newRequestMapping.getFinalSink().getName();
        final String healTopic = newRequestMapping.getAllTopics().get(finalSink).getParents().get(0);
        final String oldJustBeforeSinkTopic = newRequestMapping.getAllTopics().get(healTopic).getParents().get(0);

        retrievePipeline(healRequest.getLastTriedRemedy().pipeline).whenComplete((retrievedHealPipeline, throwable) -> {
            if (throwable != null) {
                log.error(throwable.toString());
            }
            else {
                ActionMsg action = new ActionMsgKafka(ActionMAPEK.CREATE,
                        EntityMAPEK.KAFKA_TOPIC,
                        healTopic);
                performAction(action);

                retrievedHealPipeline.setAssociatedRequestID(healRequest.getConcernedRequest());
                retrievedHealPipeline.setTempID(pipelineAssociatedToFinalSink.getTempID());
                retrievedHealPipeline.setOptionsForMAPEKReporting(monitorActor, reportIntervalRateAndQoO);
                retrievedHealPipeline.setOptionsForQoOComputation(new MySpecificQoOAttributeComputation(), pipelineAssociatedToFinalSink.getQooParams());

                // We set all customizable params of the retrieved pipeline for the healing
                healRequest.getLastParamsForRemedies().forEach(retrievedHealPipeline::setCustomizableParameter);

                Set<String> fromTopics = new HashSet<>();
                fromTopics.add(oldJustBeforeSinkTopic);
                ActionMsg action2 = new ActionMsgPipeline(ActionMAPEK.APPLY,
                        EntityMAPEK.PIPELINE,
                        retrievedHealPipeline,
                        newRequestMapping.getFinalSink().getObservationLevel(),
                        fromTopics,
                        healTopic,
                        newRequestMapping.getRequest_id(),
                        newRequestMapping.getConstructedFromRequest(),
                        -1);

                performAction(action2);

                retrievePipeline("OutputPipeline").whenComplete((outputPipeline, throwable2) -> {
                    if (throwable2 != null) {
                        log.error(throwable2.toString());
                    } else {
                        // The OutputPipeline configuration does not change so we perform an exact copy
                        ((OutputPipeline) outputPipeline).copyConfiguration((OutputPipeline) pipelineAssociatedToFinalSink);

                        Set<String> fromTopics2 = new HashSet<>();
                        fromTopics2.add(healTopic);
                        ActionMsg action3 = new ActionMsgPipeline(ActionMAPEK.APPLY,
                                EntityMAPEK.PIPELINE,
                                outputPipeline,
                                newRequestMapping.getFinalSink().getObservationLevel(),
                                fromTopics2,
                                finalSink,
                                newRequestMapping.getRequest_id(),
                                newRequestMapping.getConstructedFromRequest(),
                                -1);

                        performAction(action3);
                    }
                });
            }
        });
    }

    /**
     * Method to delete any existing heal topic + stop the last actor (that publishes to finalSink).
     * This method does not delete sinkTopic!
     * @param requestMappingToIterate
     * @param isARollback
     * @return the old OutputPipeline that was used for producing observations to the old sinkTopic.
     */
    private IPipeline stopHealAndSinkTopics(RequestMapping requestMappingToIterate, boolean isARollback) {
        IPipeline pipelineAssociatedToFinalSink = enforcedPipelines.get(mappingTopicsActors.get(requestMappingToIterate.getFinalSink().getName()));
        Map<String, TopicEntity> allTopics = requestMappingToIterate.getAllTopics();
        TopicEntity currTopic = requestMappingToIterate.getPrimarySources().get(0);

        Set<ActorRef> actorsToShutDown = new HashSet<>();
        Set<String> kafkaTopicsToDelete = new HashSet<>();
        Set<String> kafkaTopicsToReset = new HashSet<>();
        while (currTopic != null) {
            if (currTopic.isForHeal()) {
                String currActorPathName = mappingTopicsActors.get(currTopic.getName());
                execActorsCount.put(currActorPathName, execActorsCount.get(currActorPathName) - 1);
                if (isARollback) {
                    kafkaTopicsToDelete.add(currTopic.getName());
                }
                else {
                    kafkaTopicsToReset.add(currTopic.getName());
                }
                actorsToShutDown.add(execActorsRefs.get(currActorPathName));
            }
            else if (currTopic.isSink()) {
                String currActorPathName = mappingTopicsActors.get(currTopic.getName());
                execActorsCount.put(currActorPathName, execActorsCount.get(currActorPathName) - 1);
                actorsToShutDown.add(execActorsRefs.get(currActorPathName));
            }

            if (currTopic.getChildren().size() > 0) {
                currTopic = allTopics.get(currTopic.getChildren().get(0));
            }
            else {
                currTopic = null;
            }
        }

        for (ActorRef actorRefToStop : actorsToShutDown) {
            gracefulStop(actorRefToStop);
            execActorsCount.remove(actorRefToStop.path().name());
            execActorsRefs.remove(actorRefToStop.path().name());
            monitorActor.tell(new SymptomMsgRemovedPipeline(
                    SymptomMAPEK.REMOVED,
                    EntityMAPEK.PIPELINE,
                    enforcedPipelines.get(actorRefToStop.path().name()).getUniqueID()),
                    getSelf());
            enforcedPipelines.remove(actorRefToStop.path().name());
            actorPathRefs.get(requestMappingToIterate.getRequest_id()).remove(actorRefToStop.path().name());
        }

        // We clean up the unused topics
        for (String topic : kafkaTopicsToDelete) {
            performAction(new ActionMsgKafka(ActionMAPEK.DELETE, EntityMAPEK.KAFKA_TOPIC, topic));
            mappingTopicsActors.remove(topic);
        }
        for (String topic : kafkaTopicsToReset) {
            performAction(new ActionMsgKafka(ActionMAPEK.RESET, EntityMAPEK.KAFKA_TOPIC, topic));
            mappingTopicsActors.remove(topic);
        }

        mappingTopicsActors.remove(requestMappingToIterate.getFinalSink().getName()); // we remove the last sink from database only (not the Kafka topic)

        return pipelineAssociatedToFinalSink;
    }

    private void resetRequest(RequestMapping oldRequestMappingToIterate, RequestMapping newRequestMappingToIterate) {
        // We stop the sink and heal topics currently enforced
        IPipeline pipelineAssociatedToFinalSink = stopHealAndSinkTopics(oldRequestMappingToIterate, true);

        final String finalSink = newRequestMappingToIterate.getFinalSink().getName();
        final String oldJustBeforeSinkTopic = newRequestMappingToIterate.getAllTopics().get(finalSink).getParents().get(0);

        retrievePipeline("OutputPipeline").whenComplete((outputPipeline, throwable) -> {
            if (throwable != null) {
                log.error(throwable.toString());
            }
            else {
                // The OutputPipeline configuration does not change so we perform an exact copy
                ((OutputPipeline) outputPipeline).copyConfiguration((OutputPipeline) pipelineAssociatedToFinalSink);

                Set<String> fromTopics = new HashSet<>();
                fromTopics.add(oldJustBeforeSinkTopic);
                ActionMsg action = new ActionMsgPipeline(ActionMAPEK.APPLY,
                        EntityMAPEK.PIPELINE,
                        outputPipeline,
                        newRequestMappingToIterate.getFinalSink().getObservationLevel(),
                        fromTopics,
                        finalSink,
                        newRequestMappingToIterate.getRequest_id(),
                        newRequestMappingToIterate.getConstructedFromRequest(),
                        -1);

                performAction(action);
            }
        });
    }

}
