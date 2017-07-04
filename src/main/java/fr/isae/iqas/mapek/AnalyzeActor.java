package fr.isae.iqas.mapek;

import akka.actor.AbstractActor;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.kafka.RequestMapping;
import fr.isae.iqas.kafka.TopicEntity;
import fr.isae.iqas.model.jsonld.*;
import fr.isae.iqas.model.message.*;
import fr.isae.iqas.model.request.HealRequest;
import fr.isae.iqas.model.request.QoORequirements;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.model.request.State;
import fr.isae.iqas.utils.QualityUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.bson.types.ObjectId;
import scala.concurrent.duration.FiniteDuration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static akka.dispatch.Futures.future;
import static fr.isae.iqas.model.message.MAPEKenums.*;
import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK.REQUEST;
import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK.SENSOR;
import static fr.isae.iqas.model.message.MAPEKenums.RFCMAPEK.HEAL;
import static fr.isae.iqas.model.message.MAPEKenums.SymptomMAPEK.*;
import static fr.isae.iqas.model.observation.ObservationLevel.RAW_DATA;
import static fr.isae.iqas.model.quality.QoOAttribute.OBS_RATE;
import static fr.isae.iqas.model.request.State.Status.ENFORCED;
import static fr.isae.iqas.model.request.State.Status.HEALED;
import static fr.isae.iqas.utils.ActorUtils.getPlanActorFromMAPEKchild;

/**
 * Created by an.auger on 13/09/2016.
 */

public class AnalyzeActor extends AbstractActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final static int INC_STEP_FOR_INTEGER_PARAM = 1;
    private final static int DEC_STEP_FOR_INTEGER_PARAM = 1;

    private TopicList topicList;
    private MongoController mongoController;
    private FusekiController fusekiController;

    private FiniteDuration tickMAPEK;
    private long symptomLifetime;
    private long observeDuration;
    private int max_retries;
    private int maxSymptomsBeforeAction;

    private Map<String, HealRequest> currentlyHealedRequest; // RequestIDs <-> HealRequests
    private Set<String> requestsToIgnore;

    private Map<String, CircularFifoBuffer> receivedObsRateSymptoms; // UniquePipelineIDs <-> [<SymptomMsg,received date>]
    private Map<String, CircularFifoBuffer> receivedQoOSymptoms; // RequestIDs <-> [<SymptomMsg,received date>]

    public AnalyzeActor(Properties prop, MongoController mongoController, FusekiController fusekiController) {
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;

        this.tickMAPEK = new FiniteDuration(Long.valueOf(prop.getProperty("tick_mapek_seconds")), TimeUnit.SECONDS);
        this.symptomLifetime = Long.valueOf(prop.getProperty("symptom_lifetime_seconds")) * 1000;
        this.observeDuration = Integer.valueOf(prop.getProperty("max_time_to_observe_effect_seconds")) * 1000;
        this.max_retries = Integer.parseInt(prop.getProperty("max_number_retries"));
        this.maxSymptomsBeforeAction = Integer.parseInt(prop.getProperty("nb_symptoms_before_action"));

        this.currentlyHealedRequest = new ConcurrentHashMap<>();
        this.receivedObsRateSymptoms = new ConcurrentHashMap<>();
        this.receivedQoOSymptoms = new ConcurrentHashMap<>();
        this.requestsToIgnore = new HashSet<>();
    }

    @Override
    public void preStart() {
        future(() -> fusekiController.findAllTopics(), context().dispatcher())
                .onComplete(new OnComplete<TopicList>() {
                    public void onComplete(Throwable throwable, TopicList topicListResult) {
                        if (throwable == null) { // Only continue if there is no error so far
                            topicList = topicListResult;
                        }
                    }
                }, context().dispatcher());

        getContext().system().scheduler().scheduleOnce(
                tickMAPEK,
                getSelf(), "tick", getContext().dispatcher(), getSelf());
    }

    // override postRestart so we don't call preStart and schedule a new message
    @Override
    public void postRestart(Throwable reason) {
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(String.class, this::actionsOnStringMsg)
                .match(SymptomMsgRequest.class, this::actionsOnSymptomMsgRequestMsg)
                .match(SymptomMsgObsRate.class, this::actionsOnSymptomMsgObsRateMsg)
                .match(SymptomMsgConnectionReport.class, this::actionsOnSymptomMsgConnectionReportMsg)
                .match(SymptomMsg.class, this::actionsOnSymptomMsg)
                .match(TerminatedMsg.class, this::stopThisActor)
                .build();
    }

    private void actionsOnStringMsg(String msg) {
        // Tick messages
        if (msg.equals("tick")) {
            // send another periodic tick after the specified delay
            getContext().system().scheduler().scheduleOnce(
                    tickMAPEK,
                    getSelf(), "tick", getContext().dispatcher(), getSelf());

            clearExpiredSymptoms(receivedObsRateSymptoms, symptomLifetime);
            clearExpiredSymptoms(receivedQoOSymptoms, symptomLifetime);
            checkIfSomeHealedRequestsAreNowStable();
        }
    }

    private void actionsOnSymptomMsgRequestMsg(SymptomMsgRequest msg) {
        // SymptomMsg messages - Requests
        if (msg.getAbout() == REQUEST) {
            log.info("Received Symptom : {} {} {}", msg.getSymptom(), msg.getAbout(), msg.getAttachedRequest().toString());
            Request requestTemp = msg.getAttachedRequest();

            if (requestTemp.getCurrent_status() == State.Status.SUBMITTED) { // Valid Request
                mongoController.getExactSameRequestsAs(requestTemp).whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error(throwable.toString());
                    } else if (result.size() > 0) { // The incoming request may reuse existing enforced requests
                        String tempIDForPipelines = new ObjectId().toString();
                        Request similarRequest = result.get(0);

                        mongoController.getSpecificRequestMapping(similarRequest.getRequest_id()).whenComplete((similarMappings, throwable2) -> {
                            if (throwable2 != null) {
                                log.error(throwable2.toString());
                            } else {
                                similarMappings.addDependentRequest(requestTemp);

                                RequestMapping requestMapping = new RequestMapping(requestTemp.getApplication_id(), requestTemp.getRequest_id());
                                requestMapping.setConstructedFromRequest(similarRequest.getRequest_id());

                                String fromTopicName = similarMappings.getFinalSink().getParents().get(0);
                                TopicEntity fromTopic = new TopicEntity(similarMappings.getAllTopics().get(fromTopicName));
                                // Cleaning retrieved TopicEntity object
                                fromTopic.getChildren().clear();
                                fromTopic.getEnforcedPipelines().clear();
                                fromTopic.setSource(fromTopicName);
                                requestMapping.getAllTopics().put(fromTopic.getName(), fromTopic);

                                TopicEntity sinkForApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id(), requestTemp.getObs_level());
                                requestMapping.getAllTopics().put(sinkForApp.getName(), sinkForApp);
                                sinkForApp.setSink(requestTemp.getApplication_id());

                                requestMapping.addLink(fromTopic.getName(), sinkForApp.getName(), "OutputPipeline_" + tempIDForPipelines);

                                mongoController.putRequestMapping(requestMapping).whenComplete((result1, throwable1) -> {
                                    mongoController.updateRequestMapping(similarRequest.getRequest_id(), similarMappings).whenComplete((result3, throwable3) -> {
                                        tellToPlanActor(new RFCMsgRequestCreate(RFCMAPEK.CREATE, REQUEST, requestTemp, requestMapping));
                                    });
                                });
                            }
                        });
                    } else {  // The incoming request has no common points with the enforced ones
                        String tempIDForPipelines = new ObjectId().toString();
                        RequestMapping requestMapping = new RequestMapping(requestTemp.getApplication_id(), requestTemp.getRequest_id());

                        TopicEntity topicJustBeforeSink = new TopicEntity(requestTemp.getTopic() + "_" + requestTemp.getLocation() + "_" + requestTemp.getAbbrvObsLevel(),
                                requestTemp.getObs_level());
                        requestMapping.getAllTopics().put(topicJustBeforeSink.getName(), topicJustBeforeSink);

                        TopicEntity sinkForApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id(),
                                requestTemp.getObs_level());
                        sinkForApp.setSink(requestTemp.getApplication_id());
                        requestMapping.getAllTopics().put(sinkForApp.getName(), sinkForApp);

                        if (requestTemp.getTopic().equals("ALL")) { // (Location = x, Topic = ALL) or (Location = ALL, Topic = ALL)
                            for (Topic topicObject : topicList.topics) {
                                String topicName = topicObject.topic.split("#")[1];
                                TopicEntity topicBase = new TopicEntity(topicName, RAW_DATA);
                                topicBase.setSource(topicName);
                                requestMapping.getAllTopics().put(topicName, topicBase);
                                requestMapping.addLink(topicBase.getName(), sinkForApp.getName(), "IngestPipeline_" + tempIDForPipelines);
                            }
                        } else { // (Location = x, Topic = y) or (Location = ALL, Topic = x)
                            TopicEntity topicBase = new TopicEntity(requestTemp.getTopic(), RAW_DATA);
                            topicBase.setSource(requestTemp.getTopic());
                            requestMapping.getAllTopics().put(topicBase.getName(), topicBase);
                            requestMapping.addLink(topicBase.getName(), topicJustBeforeSink.getName(), "IngestPipeline_" + tempIDForPipelines);
                        }

                        /**
                         * Resolution of iQAS parameters before enforcing the Request
                         * This allows to have observations of higher quality more quickly.
                         *
                         * The natural order is FilterPipeline -> ThrottlePipeline -> OutputPipeline
                         */

                        TopicEntity lastQoOTopic = topicJustBeforeSink;
                        int counterQoO = 0;

                        // If there is some QoO constraints about OBS_ACCURACY, we add a FilterPipeline
                        if (requestTemp.getQooConstraints().getIqas_params().containsKey("threshold_min")
                                || requestTemp.getQooConstraints().getIqas_params().containsKey("threshold_max")) {
                            counterQoO += 1;

                            TopicEntity accurObsTopic = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id() + "_QOO" + String.valueOf(counterQoO),
                                    requestTemp.getObs_level());
                            requestMapping.getAllTopics().put(accurObsTopic.getName(), accurObsTopic);

                            requestMapping.addLink(lastQoOTopic.getName(), accurObsTopic.getName(), "FilterPipeline_" + tempIDForPipelines);
                            lastQoOTopic = accurObsTopic;
                        }
                        // If there is some QoO constraints about OBS_RATE, we add a ThrottlePipeline
                        if (requestTemp.getQooConstraints().getIqas_params().containsKey("obsRate_max")) {
                            counterQoO += 1;

                            TopicEntity rateObsTopic = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id() + "_QOO" + String.valueOf(counterQoO),
                                    requestTemp.getObs_level());
                            requestMapping.getAllTopics().put(rateObsTopic.getName(), rateObsTopic);

                            requestMapping.addLink(lastQoOTopic.getName(), rateObsTopic.getName(), "ThrottlePipeline_" + tempIDForPipelines);
                            lastQoOTopic = rateObsTopic;
                        }

                        // Last edge for graph
                        // QoO constraints about OBS_FRESHNESS are enforced at this level
                        requestMapping.addLink(lastQoOTopic.getName(), sinkForApp.getName(), "OutputPipeline_" + tempIDForPipelines);

                        mongoController.putRequestMapping(requestMapping).whenComplete((result1, throwable1) -> {
                            tellToPlanActor(new RFCMsgRequestCreate(RFCMAPEK.CREATE, REQUEST, requestTemp, requestMapping));
                        });
                    }
                });
            } else if (requestTemp.getCurrent_status() == State.Status.REMOVED) { // Request deleted by the user
                tellToPlanActor(new RFCMsgRequestRemove(RFCMAPEK.REMOVE, REQUEST, requestTemp));
            }
        }
    }

    private void actionsOnSymptomMsgObsRateMsg(SymptomMsgObsRate msg) {
        // SymptomMsg messages - Obs Rate
        if (msg.getSymptom() == TOO_LOW && msg.getAbout() == EntityMAPEK.OBS_RATE) {
            log.info("Received Symptom : {} {} {} {}", msg.getSymptom(), msg.getAbout(), msg.getUniqueIDPipeline(), msg.getConcernedRequests().toString());
            receivedObsRateSymptoms.putIfAbsent(msg.getUniqueIDPipeline(), new CircularFifoBuffer(maxSymptomsBeforeAction));
            receivedObsRateSymptoms.get(msg.getUniqueIDPipeline()).add(new MAPEKSymptomMsgWithDate(msg));

            if (receivedObsRateSymptoms.get(msg.getUniqueIDPipeline()).size() >= maxSymptomsBeforeAction) {
                for (String request_id : msg.getConcernedRequests()) {

                    log.info("Searching for Request " + request_id);

                    if (!requestsToIgnore.contains(request_id)) {
                        currentlyHealedRequest.putIfAbsent(request_id, new HealRequest(request_id, msg.getUniqueIDPipeline(), observeDuration));
                        HealRequest currHealRequest = currentlyHealedRequest.get(request_id);

                        if (currHealRequest.canPerformHeal()) {
                            log.info("We can perform heal for Request " + request_id);
                            mongoController.getSpecificRequest(request_id).whenComplete((retrievedRequest, throwable) -> {
                                if (throwable == null) {

                                    mongoController.getSpecificRequestMapping(retrievedRequest.getRequest_id()).whenComplete((oldRequestMapping, throwable2) -> {
                                        RequestMapping newRequestMapping = new RequestMapping(oldRequestMapping);

                                        if (throwable2 == null) {

                                            future(() -> fusekiController.findMatchingPipelinesToHeal(OBS_RATE, retrievedRequest.getQooConstraints().getInterested_in()), context().dispatcher())
                                                    .onComplete(new OnComplete<QoOPipelineList>() {
                                                        public void onComplete(Throwable throwable3, QoOPipelineList qoOPipelineList) {

                                                            if (throwable3 == null && qoOPipelineList.qoOPipelines.size() > 0) { // Only continue if there was no error so far
                                                                QoOPipeline qoOPipelineToApplyForHeal = qoOPipelineList.qoOPipelines.get(0); // TODO not static decision
                                                                if (currHealRequest.getLastHealFor() == null
                                                                        || !currHealRequest.getLastHealFor().equals(OBS_RATE)
                                                                        || (currHealRequest.getLastHealFor().equals(OBS_RATE) && currHealRequest.getRetries() < max_retries)
                                                                        || !currHealRequest.hasAlreadyBeenTried(OBS_RATE, qoOPipelineToApplyForHeal)) {

                                                                    retrievedRequest.updateState(HEALED);
                                                                    retrievedRequest.addLog("On the point to heal request with " + qoOPipelineToApplyForHeal.pipeline +
                                                                            " for the time #" + String.valueOf(currHealRequest.getRetries() + 1));

                                                                    log.info("On the point to heal request with " + qoOPipelineToApplyForHeal.pipeline +
                                                                            " for the time #" + String.valueOf(currHealRequest.getRetries() + 1));

                                                                    QoOCustomizableParam qoOCustomizableParam = qoOPipelineToApplyForHeal.customizable_params.get(0);
                                                                    Map<String, String> newHealParams = new ConcurrentHashMap<>();

                                                                    if (currHealRequest.getRetries() == 0) {
                                                                        newHealParams.put(qoOCustomizableParam.param_name, qoOCustomizableParam.paramInitialValue);
                                                                    } else if (qoOCustomizableParam.paramType.equals("Integer")) {

                                                                        String variationDir = decideToIncreaseOrIncrease(msg.getSymptom(), qoOCustomizableParam);
                                                                        if (variationDir != null) {
                                                                            Map<String, String> oldParams = currHealRequest.getLastParamsForRemedies();
                                                                            for (Map.Entry<String, String> entry : oldParams.entrySet()) {
                                                                                switch (variationDir) {
                                                                                    case "HIGH":
                                                                                        newHealParams.put(entry.getKey(), String.valueOf(Integer.valueOf(entry.getValue()) + INC_STEP_FOR_INTEGER_PARAM));
                                                                                        break;
                                                                                    case "LOW":
                                                                                        newHealParams.put(entry.getKey(), String.valueOf(Integer.valueOf(entry.getValue()) - DEC_STEP_FOR_INTEGER_PARAM));
                                                                                        break;
                                                                                    default:
                                                                                        break;
                                                                                }
                                                                            }
                                                                        } else {
                                                                            newHealParams = currHealRequest.getLastParamsForRemedies();
                                                                        }
                                                                    }

                                                                    currHealRequest.performHeal(OBS_RATE, qoOPipelineToApplyForHeal, newHealParams);
                                                                    currentlyHealedRequest.put(request_id, currHealRequest);
                                                                    newRequestMapping.resetRequestHeal();
                                                                    newRequestMapping.healRequestWith(qoOPipelineToApplyForHeal, currHealRequest.getRetries());

                                                                    // We save changes into MongoDB
                                                                    mongoController.updateRequestMapping(retrievedRequest.getRequest_id(), newRequestMapping).whenComplete((result4, throwable4) -> {
                                                                        if (throwable4 == null) {
                                                                            tellToPlanActor(new RFCMsgQoOAttr(HEAL, REQUEST, currHealRequest, oldRequestMapping, newRequestMapping));
                                                                            mongoController.updateRequest(retrievedRequest.getRequest_id(), retrievedRequest).whenComplete((result5, throwable5) -> {
                                                                                if (throwable5 != null) {
                                                                                    log.error("Update of the Request " + retrievedRequest.getRequest_id() + " has failed");
                                                                                }
                                                                            });
                                                                        }
                                                                    });
                                                                } else if (!requestsToIgnore.contains(retrievedRequest.getRequest_id())) {
                                                                    // Already tried this solution and does not seem to work...
                                                                    requestsToIgnore.add(retrievedRequest.getRequest_id());
                                                                    currentlyHealedRequest.remove(retrievedRequest.getRequest_id());
                                                                    retrievedRequest.addLog("Max retry attempts (" + String.valueOf(max_retries) + ") reached to heal this request. " +
                                                                            "Last heal was tried with the pipeline " + currHealRequest.getLastTriedRemedy().pipeline + " to improve " +
                                                                            currHealRequest.getLastHealFor().toString() + " with the following params: " +
                                                                            currHealRequest.getLastParamsForRemedies().toString() + ".");
                                                                    if (retrievedRequest.getQooConstraints().getSla_level().equals(QoORequirements.SLALevel.GUARANTEED)) {
                                                                        retrievedRequest.addLog("Removing this request since it has a GUARANTEED Service Level Agreement.");
                                                                        retrievedRequest.updateState(State.Status.REMOVED);
                                                                        tellToPlanActor(new RFCMsgRequestRemove(RFCMAPEK.REMOVE, REQUEST, retrievedRequest));
                                                                    } else {
                                                                        retrievedRequest.addLog("Impossible to ensure an acceptable QoO level for this request. " +
                                                                                "However, this request won't be removed since it has a BEST EFFORT Service Level Agreement.");
                                                                        retrievedRequest.updateState(ENFORCED);
                                                                    }

                                                                    newRequestMapping.resetRequestHeal();

                                                                    // We save changes into MongoDB
                                                                    mongoController.updateRequestMapping(retrievedRequest.getRequest_id(), newRequestMapping).whenComplete((result4, throwable4) -> {
                                                                        if (throwable4 == null) {
                                                                            tellToPlanActor(new RFCMsgQoOAttr(RFCMAPEK.RESET, REQUEST, currHealRequest, oldRequestMapping, newRequestMapping));
                                                                            mongoController.updateRequest(retrievedRequest.getRequest_id(), retrievedRequest).whenComplete((result5, throwable5) -> {
                                                                                if (throwable5 != null) {
                                                                                    log.error("Update of the Request " + retrievedRequest.getRequest_id() + " has failed");
                                                                                }
                                                                            });
                                                                        }
                                                                    });
                                                                }
                                                            } else if (!requestsToIgnore.contains(retrievedRequest.getRequest_id())) {
                                                                // No suitable Pipeline for healing, removing Request if its level is "GUARANTEED"
                                                                requestsToIgnore.add(retrievedRequest.getRequest_id());
                                                                currentlyHealedRequest.remove(retrievedRequest.getRequest_id());
                                                                if (retrievedRequest.getQooConstraints().getSla_level().equals(QoORequirements.SLALevel.GUARANTEED)) {
                                                                    retrievedRequest.addLog("No suitable pipeline for healing. " +
                                                                            "Rejecting this request since it has a GUARANTEED Service Level Agreement.");
                                                                    retrievedRequest.updateState(State.Status.REMOVED);
                                                                    tellToPlanActor(new RFCMsgRequestRemove(RFCMAPEK.REMOVE, REQUEST, retrievedRequest));
                                                                } else {
                                                                    retrievedRequest.addLog("No suitable pipeline for healing. " +
                                                                            "However, this request won't be removed since it has a BEST EFFORT Service Level Agreement.");
                                                                    retrievedRequest.updateState(ENFORCED);
                                                                }
                                                                // We save changes into MongoDB
                                                                mongoController.updateRequest(retrievedRequest.getRequest_id(), retrievedRequest).whenComplete((result, throwable) -> {
                                                                    if (throwable != null) {
                                                                        log.error("Update of the Request " + retrievedRequest.getRequest_id() + " has failed");
                                                                    }
                                                                });
                                                            }
                                                        }
                                                    }, context().dispatcher());
                                        } else {
                                            log.warning("Unable to retrieve Request Mapping for Request " + request_id + ". Operation skipped!");
                                        }
                                    });
                                } else {
                                    log.warning("Unable to retrieve request " + request_id + ". Operation skipped!");
                                }
                            });
                        }
                    }
                }
            }
        }
    }

    private void actionsOnSymptomMsgConnectionReportMsg(SymptomMsgConnectionReport msg) {
        // SymptomMsg messages - Virtual Sensors
        if (msg.getSymptom() == CONNECTION_REPORT && msg.getAbout() == SENSOR) {
            log.info("Received Symptom : {} {} {}", msg.getSymptom(), msg.getAbout(), msg.getConnectedSensors().toString());
            // Only to log the symptom. Not doing anything since it only affects future iQAS Requests.
        }
    }

    private void actionsOnSymptomMsg(SymptomMsg msg) {
        // SymptomMsg messages - Virtual Sensors
        if (msg.getSymptom() == UPDATED && msg.getAbout() == SENSOR) {
            log.info("Received Symptom : {} {}", msg.getSymptom(), msg.getAbout());
            future(() -> fusekiController.findAllTopics(), context().dispatcher())
                    .onComplete(new OnComplete<TopicList>() {
                        public void onComplete(Throwable throwable, TopicList topicListResult) {
                            if (throwable == null) { // Only continue if there was no error so far
                                topicList = topicListResult;
                            }
                        }
                    }, context().dispatcher());
            tellToPlanActor(new RFCMsg(RFCMAPEK.UPDATE, SENSOR));
        }
    }

    private void stopThisActor(TerminatedMsg msg) {
        if (msg.getTargetToStop().path().equals(getSelf().path())) {
            log.info("Received TerminatedMsg message: " + msg);
            getContext().stop(self());
        }
    }

    private void tellToPlanActor(RFCMsg rfcMsg) {
        getPlanActorFromMAPEKchild(getContext(), getSelf()).tell(rfcMsg, getSelf());
    }

    private void checkIfSomeHealedRequestsAreNowStable() {
        for (Iterator<Map.Entry<String, HealRequest>> it = currentlyHealedRequest.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, HealRequest> pair = it.next();
            if (receivedObsRateSymptoms.containsKey(pair.getValue().getUniqueIDPipeline())
                    && receivedObsRateSymptoms.get(pair.getValue().getUniqueIDPipeline()).size() == 0
                    && System.currentTimeMillis() - pair.getValue().getHealStartDate() > observeDuration) {

                receivedObsRateSymptoms.remove(pair.getValue().getUniqueIDPipeline());

                // We save changes into MongoDB
                mongoController.getSpecificRequest(pair.getKey()).whenComplete((retrievedRequest, throwable) -> {
                    if (throwable == null) {
                        retrievedRequest.updateState(ENFORCED);
                        retrievedRequest.addLog("Heal process has been stable for enough time. Request's state is returning to ENFORCED.");
                        mongoController.updateRequest(retrievedRequest.getRequest_id(), retrievedRequest).whenComplete((result, throwable2) -> {
                            if (throwable2 != null) {
                                log.error("Update of the Request " + retrievedRequest.getRequest_id() + " has failed");
                            }
                        });
                    }
                });
                it.remove();
            }
        }
    }

    /**
     * This method is called at each MAPE-K tick in order to delete the expired Symptom messages stored in buffers
     *
     * @param mapToCheck
     * @param symptomLifetime
     */
    private void clearExpiredSymptoms(Map<String, CircularFifoBuffer> mapToCheck, long symptomLifetime) {
        for (Iterator<Map.Entry<String, CircularFifoBuffer>> it1 = mapToCheck.entrySet().iterator(); it1.hasNext(); ) {
            Map.Entry<String, CircularFifoBuffer> pair = it1.next();
            CircularFifoBuffer buffer = pair.getValue();
            for (Iterator it2 = buffer.iterator(); it2.hasNext(); ) {
                MAPEKSymptomMsgWithDate o = (MAPEKSymptomMsgWithDate) it2.next();
                if (System.currentTimeMillis() - o.getSymptomCreationDate() > symptomLifetime) {
                    it2.remove();
                }
            }
        }
    }

    /**
     * @param symptomMAPEK
     * @param qoOCustomizableParam
     * @return String "HIGH" or "LOW" or null if unable to find a suitable QoOEffect
     */
    private String decideToIncreaseOrIncrease(SymptomMAPEK symptomMAPEK, QoOCustomizableParam qoOCustomizableParam) {
        String realSymptom = symptomMAPEK.toString().split("_")[1];
        for (QoOEffect qoOEffect : qoOCustomizableParam.has) {
            if (qoOEffect.qooAttributeVariation.equals(QualityUtils.inverseOfVariation(realSymptom))) {
                return qoOEffect.paramVariation;
            }
        }
        return null;
    }
}
