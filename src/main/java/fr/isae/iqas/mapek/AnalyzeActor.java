package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.kafka.RequestMapping;
import fr.isae.iqas.kafka.TopicEntity;
import fr.isae.iqas.model.jsonld.QoOPipelineList;
import fr.isae.iqas.model.jsonld.Topic;
import fr.isae.iqas.model.jsonld.TopicList;
import fr.isae.iqas.model.message.MAPEKSymptomMsgWithDate;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.request.HealRequest;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.model.request.State;
import fr.isae.iqas.utils.ActorUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.bson.types.ObjectId;
import scala.concurrent.duration.FiniteDuration;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static akka.dispatch.Futures.future;
import static fr.isae.iqas.model.message.MAPEKInternalMsg.*;
import static fr.isae.iqas.model.message.MAPEKInternalMsg.SymptomMAPEK.*;
import static fr.isae.iqas.model.observation.ObservationLevel.RAW_DATA;
import static fr.isae.iqas.model.quality.QoOAttribute.OBS_RATE;

/**
 * Created by an.auger on 13/09/2016.
 */

public class AnalyzeActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private final static int MAX_SYMPTOMS_BEFORE_ACTION = 5;
    private int max_retries;

    private TopicList topicList;
    private MongoController mongoController;
    private FusekiController fusekiController;

    private FiniteDuration tickMAPEK;
    private FiniteDuration symptomLifetime;
    private long observeDuration;

    private Map<String, HealRequest> currentlyHealedRequest; // RequestIDs <-> HealRequests

    private Map<String, CircularFifoBuffer> receivedObsRateSymptoms; // UniquePipelineIDs <-> [<SymptomMsg,received date>]
    private Map<String, CircularFifoBuffer> receivedQoOSymptoms; // RequestIDs <-> [<SymptomMsg,received date>]

    public AnalyzeActor(Properties prop, MongoController mongoController, FusekiController fusekiController) {
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;

        this.tickMAPEK = new FiniteDuration(Long.valueOf(prop.getProperty("tick_mapek_seconds")), TimeUnit.SECONDS);
        this.symptomLifetime = new FiniteDuration(Long.valueOf(prop.getProperty("symptom_lifetime_seconds")), TimeUnit.SECONDS);
        this.observeDuration = Integer.valueOf(prop.getProperty("max_time_to_observe_effect_seconds")) * 1000;
        this.max_retries = Integer.parseInt(prop.getProperty("max_number_retries"));

        this.receivedObsRateSymptoms = new ConcurrentHashMap<>();
        this.receivedQoOSymptoms = new ConcurrentHashMap<>();
    }

    @Override
    public void preStart() {
        future(() -> fusekiController._findAllTopics(), context().dispatcher())
                .onComplete(new OnComplete<TopicList>() {
                    public void onComplete(Throwable throwable, TopicList topicListResult) {
                        if (throwable == null) { // Only continue if there was no error so far
                            topicList = topicListResult;
                        }
                    }
                }, context().dispatcher());

        getContext().system().scheduler().scheduleOnce(
                tickMAPEK,
                getSelf(), "tick", getContext().dispatcher(), getSelf());

        // TODO remove
        /*ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JsonldModule(mapper::createObjectNode));
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        JsonldResourceBuilder builder = JsonldResource.Builder.create();

        QoOPipelineList t = fusekiController._findMatchingPipelinesToHeal(OBS_FRESHNESS, Arrays.asList(OBS_ACCURACY, OBS_RATE));
        try {
            log.info(mapper.writeValueAsString(t));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }*/
    }

    // override postRestart so we don't call preStart and schedule a new message
    @Override
    public void postRestart(Throwable reason) {
    }

    @Override
    public void onReceive(Object message) throws Exception {
        // Tick messages
        if (message.equals("tick")) {
            // send another periodic tick after the specified delay
            getContext().system().scheduler().scheduleOnce(
                    tickMAPEK,
                    getSelf(), "tick", getContext().dispatcher(), getSelf());

            clearExpiredSymptoms(receivedObsRateSymptoms, symptomLifetime);
            clearExpiredSymptoms(receivedQoOSymptoms, symptomLifetime);
        }
        // SymptomMsg messages [received from MonitorActor]
        else if (message instanceof SymptomMsg) {
            SymptomMsg symptomMsg = (SymptomMsg) message;

            // SymptomMsg messages - Requests
            if (symptomMsg.getAbout() == EntityMAPEK.REQUEST) {
                log.info("Received Symptom : {} {} {}", symptomMsg.getSymptom(), symptomMsg.getAbout(), symptomMsg.getAttachedRequest().toString());
                Request requestTemp = symptomMsg.getAttachedRequest();

                if (requestTemp.getCurrent_status() == State.Status.SUBMITTED) { // Valid Request
                    mongoController.getExactSameRequestsAs(requestTemp).whenComplete((result, throwable) -> {
                        if (throwable != null) {
                            log.error(throwable.toString());
                        }
                        else if (result.size() > 0) { // The incoming request may reuse existing enforced requests
                            String tempIDForPipelines = new ObjectId().toString();
                            Request similarRequest = result.get(0);

                            mongoController.getSpecificRequestMapping(similarRequest.getRequest_id()).whenComplete((result2, throwable2) -> {
                                if (throwable2 != null || result2.size() <= 0) {
                                    if (throwable2 != null) {
                                        log.error(throwable2.toString());
                                    }
                                }
                                else {
                                    RequestMapping similarMappings = result2.get(0);
                                    similarMappings.addDependentRequest(requestTemp);

                                    RequestMapping requestMapping = new RequestMapping(requestTemp.getRequest_id());
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
                                            tellToPlanActor(new RFCMsg(RFCMAPEK.CREATE, EntityMAPEK.REQUEST, requestTemp, requestMapping));
                                        });
                                    });
                                }
                            });
                        }
                        else {  // The incoming request has no common points with the enforced ones
                            String tempIDForPipelines = new ObjectId().toString();
                            RequestMapping requestMapping = new RequestMapping(requestTemp.getRequest_id());

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
                                    requestMapping.addLink(topicBase.getName(), topicJustBeforeSink.getName(), "IngestPipeline_" + tempIDForPipelines);
                                }
                            }
                            else { // (Location = x, Topic = y) or (Location = ALL, Topic = x)
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
                            if ( requestTemp.getQooConstraints().getIqas_params().containsKey("threshold_min")
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

                                requestMapping.addLink(topicJustBeforeSink.getName(), rateObsTopic.getName(), "ThrottlePipeline_" + tempIDForPipelines);
                                lastQoOTopic = rateObsTopic;
                            }

                            // Last edge for graph
                            // QoO constraints about OBS_FRESHNESS are enforced at this level
                            requestMapping.addLink(lastQoOTopic.getName(), sinkForApp.getName(), "OutputPipeline_" + tempIDForPipelines);

                            mongoController.putRequestMapping(requestMapping).whenComplete((result1, throwable1) -> {
                                tellToPlanActor(new RFCMsg(RFCMAPEK.CREATE, EntityMAPEK.REQUEST, requestTemp, requestMapping));
                            });
                        }
                    });
                }
                else if (requestTemp.getCurrent_status() == State.Status.REMOVED) { // Request deleted by the user
                    tellToPlanActor(new RFCMsg(RFCMAPEK.REMOVE, EntityMAPEK.REQUEST, requestTemp));
                }
            }
            // TODO QoO adaptation
            // SymptomMsg messages - Obs Rate
            else if (symptomMsg.getSymptom() == TOO_LOW && symptomMsg.getAbout() == EntityMAPEK.OBS_RATE) {
                log.info("Received Symptom : {} {} {} {}", symptomMsg.getSymptom(), symptomMsg.getAbout(), symptomMsg.getUniqueIDPipeline(), symptomMsg.getConcernedRequests().toString());
                receivedObsRateSymptoms.putIfAbsent(symptomMsg.getUniqueIDPipeline(), new CircularFifoBuffer(MAX_SYMPTOMS_BEFORE_ACTION));
                receivedObsRateSymptoms.get(symptomMsg.getUniqueIDPipeline()).add(new MAPEKSymptomMsgWithDate(symptomMsg));

                if (receivedObsRateSymptoms.get(symptomMsg.getUniqueIDPipeline()).size() == MAX_SYMPTOMS_BEFORE_ACTION) {
                    // TODO tell Plan Actor
                    //tellToPlanActor(new RFCMsg(RFCMAPEK.INCREASE, EntityMAPEK.OBS_RATE, new HealPipeline(/* TODO */observeDuration)));
                    //receivedObsRateSymptoms.get(symptomMsg.getUniqueIDPipeline()).clear();

                    /*getContext().system().scheduler().scheduleOnce(
                            new FiniteDuration(0, TimeUnit.SECONDS),
                            getSelf(), "dkddk", getContext().dispatcher(), getSelf());*/

                    for (String request_id : symptomMsg.getConcernedRequests()) {

                        currentlyHealedRequest.putIfAbsent(request_id, new HealRequest(request_id, OBS_RATE, observeDuration));
                        HealRequest currHealRequest = currentlyHealedRequest.get(request_id);

                        if (currHealRequest.canPerformHeal() && currHealRequest.getRetries() < max_retries) {

                            currHealRequest.performHeal(OBS_RATE);
                            currentlyHealedRequest.put(request_id, currHealRequest);

                            mongoController.getSpecificRequest(request_id).whenComplete((result, throwable) -> {
                                if (throwable == null && result.size() == 1) {
                                    Request retrievedRequest = result.get(0);

                                    future(() -> fusekiController._findMatchingPipelinesToHeal(OBS_RATE, retrievedRequest.getQooConstraints().getInterested_in()), context().dispatcher())
                                            .onComplete(new OnComplete<QoOPipelineList>() {
                                                public void onComplete(Throwable throwable, QoOPipelineList qoOPipelineList) {
                                                    if (throwable == null) { // Only continue if there was no error so far
                                                        log.info("On the point to apply " + qoOPipelineList.qoOPipelines.get(0).pipeline);


                                                    }
                                                }
                                            }, context().dispatcher());
                                } else {
                                    log.warning("Unable to retrieve request " + request_id + ". Operation skipped!");
                                }
                            });
                        }
                    }
                }
            }
            // SymptomMsg messages - Virtual Sensors
            else if (symptomMsg.getSymptom() == CONNECTION_REPORT && symptomMsg.getAbout() == EntityMAPEK.SENSOR) {
                log.info("Received Symptom : {} {} {}", symptomMsg.getSymptom(), symptomMsg.getAbout(), symptomMsg.getConnectedSensors().toString());
                // Only to log the symptom. Not doing anything since it only affects future iQAS Requests.
            }
            else if (symptomMsg.getSymptom() == UPDATED && symptomMsg.getAbout() == EntityMAPEK.SENSOR) {
                log.info("Received Symptom : {} {}", symptomMsg.getSymptom(), symptomMsg.getAbout());
                future(() -> fusekiController._findAllTopics(), context().dispatcher())
                        .onComplete(new OnComplete<TopicList>() {
                            public void onComplete(Throwable throwable, TopicList topicListResult) {
                                if (throwable == null) { // Only continue if there was no error so far
                                    topicList = topicListResult;
                                }
                            }
                        }, context().dispatcher());
                tellToPlanActor(new RFCMsg(RFCMAPEK.UPDATE, EntityMAPEK.SENSOR));
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
        else if (message instanceof String) {
            log.info("Received String message: {}", message);
        }
    }

    private void tellToPlanActor(RFCMsg rfcMsg) {
        ActorUtils.getPlanActor(getContext(), getSelf()).onComplete(new OnComplete<ActorRef>() {
            @Override
            public void onComplete(Throwable t, ActorRef planActor) throws Throwable {
                if (t != null) {
                    log.error("Unable to find the PlanActor: " + t.toString());
                }
                else {
                    planActor.tell(rfcMsg, getSelf());
                }
            }
        }, getContext().dispatcher());
    }

    /**
     * This method is called at each MAPE-K tick in order to delete the expired Symptom messages stored in buffers
     * @param mapToCheck
     * @param symptomLifetime
     */
    private void clearExpiredSymptoms(Map<String,CircularFifoBuffer> mapToCheck, FiniteDuration symptomLifetime) {
        FiniteDuration now = new FiniteDuration(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        for (Iterator<Map.Entry<String, CircularFifoBuffer>> it = mapToCheck.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, CircularFifoBuffer> pair = it.next();
            CircularFifoBuffer buffer = pair.getValue();
            buffer.removeIf(o -> {
                MAPEKSymptomMsgWithDate msg = (MAPEKSymptomMsgWithDate) o;
                return now.minus(msg.getSymptomCreationDate()).gt(symptomLifetime);
            });
            if (buffer.size() == 0) {
                it.remove();
            }
        }
    }
}
