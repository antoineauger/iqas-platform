package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.util.Timeout;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.kafka.RequestMapping;
import fr.isae.iqas.kafka.TopicEntity;
import fr.isae.iqas.model.jsonld.Topic;
import fr.isae.iqas.model.jsonld.TopicList;
import fr.isae.iqas.model.message.MAPEKSymptomMsgWithDate;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.model.request.State;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.bson.types.ObjectId;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static fr.isae.iqas.model.message.MAPEKInternalMsg.*;
import static fr.isae.iqas.model.observation.ObservationLevel.RAW_DATA;

/**
 * Created by an.auger on 13/09/2016.
 */
public class AnalyzeActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Properties prop;
    private MongoController mongoController;
    private FusekiController fusekiController;

    private FiniteDuration tickMAPEK = null;
    private FiniteDuration symptomLifetime = null;
    private Map<String, CircularFifoBuffer> receivedObsRateSymptoms; // UniquePipelineIDs <-> [<SymptomMsg,received date>]
    private Map<String, CircularFifoBuffer> receivedQoOSymptoms; // RequestIDs <-> [<SymptomMsg,received date>]

    public AnalyzeActor(Properties prop, MongoController mongoController, FusekiController fusekiController) {
        this.prop = prop;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;

        this.tickMAPEK = new FiniteDuration(Long.valueOf(prop.getProperty("tick_mapek_seconds")), TimeUnit.SECONDS);
        this.symptomLifetime = new FiniteDuration(Long.valueOf(prop.getProperty("symptom_lifetime_seconds")), TimeUnit.SECONDS);

        this.receivedObsRateSymptoms = new ConcurrentHashMap<>();
        this.receivedQoOSymptoms = new ConcurrentHashMap<>();
    }

    @Override
    public void preStart() {
        getContext().system().scheduler().scheduleOnce(
                tickMAPEK,
                getSelf(), "tick", getContext().dispatcher(), getSelf());
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
        if (message instanceof SymptomMsg) {
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

                                    requestMapping.addLink(fromTopic.getName(), sinkForApp.getName(), "ForwardPipeline_" + tempIDForPipelines);

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

                            if (requestTemp.getTopic().equals("ALL")) { // (Location = x, Topic = ALL) or (Location = ALL, Topic = ALL)
                                TopicEntity topicInt = new TopicEntity(requestTemp.getTopic() + "_" + requestTemp.getLocation() + "_" + requestTemp.getAbbrvObsLevel(),
                                        requestTemp.getObs_level());
                                requestMapping.getAllTopics().put(topicInt.getName(), topicInt);

                                TopicList topicList = fusekiController._findAllTopics();
                                for (Topic topicObject : topicList.topics) {
                                    String topicName = topicObject.topic.split("#")[1];

                                    TopicEntity topicBase = new TopicEntity(topicName, RAW_DATA);
                                    topicBase.setSource(topicName);
                                    requestMapping.getAllTopics().put(topicName, topicBase);

                                    requestMapping.addLink(topicBase.getName(), topicInt.getName(), "SensorFilterPipeline_" + tempIDForPipelines);
                                }

                                TopicEntity topicObsRate = new TopicEntity(requestTemp.getTopic() + "_" + requestTemp.getLocation() + "_OBSRATE_" + requestTemp.getAbbrvObsLevel(),
                                        requestTemp.getObs_level());
                                requestMapping.getAllTopics().put(topicObsRate.getName(), topicObsRate);

                                TopicEntity sinkForApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id(),
                                        requestTemp.getObs_level());
                                sinkForApp.setSink(requestTemp.getApplication_id());
                                requestMapping.getAllTopics().put(sinkForApp.getName(), sinkForApp);

                                requestMapping.addLink(topicInt.getName(), topicObsRate.getName(), "ObsRatePipeline_" + tempIDForPipelines);

                                if (requestTemp.getObs_level().equals(RAW_DATA)) {
                                    requestMapping.addLink(topicObsRate.getName(), sinkForApp.getName(), "ForwardPipeline_" + tempIDForPipelines);
                                }
                                else {
                                    TopicEntity sinkQoObeforeApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id() + "_QOO",
                                            requestTemp.getObs_level());
                                    requestMapping.getAllTopics().put(sinkQoObeforeApp.getName(), sinkQoObeforeApp);
                                    requestMapping.addLink(topicObsRate.getName(), sinkQoObeforeApp.getName(), "QoOAnnotatorPipeline_" + tempIDForPipelines);
                                    requestMapping.addLink(sinkQoObeforeApp.getName(), sinkForApp.getName(), "ForwardPipeline_" + tempIDForPipelines);
                                }
                            }
                            else { // (Location = x, Topic = y) or (Location = ALL, Topic = x)
                                TopicEntity topicBase = new TopicEntity(requestTemp.getTopic(), RAW_DATA);
                                topicBase.setSource(requestTemp.getTopic());
                                requestMapping.getAllTopics().put(topicBase.getName(), topicBase);

                                TopicEntity topicObsRate = new TopicEntity(requestTemp.getTopic() + "_" + requestTemp.getLocation() + "_OBSRATE_" + requestTemp.getAbbrvObsLevel(),
                                        requestTemp.getObs_level());
                                requestMapping.getAllTopics().put(topicObsRate.getName(), topicObsRate);

                                TopicEntity topicInt = new TopicEntity(requestTemp.getTopic() + "_" + requestTemp.getLocation() + "_" + requestTemp.getAbbrvObsLevel(),
                                        requestTemp.getObs_level());
                                requestMapping.getAllTopics().put(topicInt.getName(), topicInt);

                                TopicEntity sinkForApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id(),
                                        requestTemp.getObs_level());
                                sinkForApp.setSink(requestTemp.getApplication_id());
                                requestMapping.getAllTopics().put(sinkForApp.getName(), sinkForApp);

                                requestMapping.addLink(topicBase.getName(), topicInt.getName(), "SensorFilterPipeline_" + tempIDForPipelines);
                                requestMapping.addLink(topicInt.getName(), topicObsRate.getName(), "ObsRatePipeline_" + tempIDForPipelines);

                                if (requestTemp.getObs_level().equals(RAW_DATA)) {
                                    requestMapping.addLink(topicObsRate.getName(), sinkForApp.getName(), "ForwardPipeline_" + tempIDForPipelines);
                                }
                                else {
                                    TopicEntity sinkQoObeforeApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id() + "_QOO",
                                            requestTemp.getObs_level());
                                    requestMapping.getAllTopics().put(sinkQoObeforeApp.getName(), sinkQoObeforeApp);
                                    requestMapping.addLink(topicObsRate.getName(), sinkQoObeforeApp.getName(), "QoOAnnotatorPipeline_" + tempIDForPipelines);
                                    requestMapping.addLink(sinkQoObeforeApp.getName(), sinkForApp.getName(), "ForwardPipeline_" + tempIDForPipelines);
                                }
                            }

                            mongoController.putRequestMapping(requestMapping).whenComplete((result1, throwable1) -> {
                                tellToPlanActor(new RFCMsg(RFCMAPEK.CREATE, EntityMAPEK.REQUEST, requestTemp, requestMapping));
                            });
                        }
                    });
                }
                else if (requestTemp.getCurrent_status() == State.Status.UPDATED) { // Existing Request updated by the user
                    // TODO Request update
                }
                else if (requestTemp.getCurrent_status() == State.Status.REMOVED) { // Request deleted by the user
                    tellToPlanActor(new RFCMsg(RFCMAPEK.REMOVE, EntityMAPEK.REQUEST, requestTemp));
                }
            }
            // TODO QoO adaptation
            // SymptomMsg messages - Obs Rate
            else if (symptomMsg.getAbout() == EntityMAPEK.OBS_RATE) {
                log.info("Received Symptom : {} {} {} {}", symptomMsg.getSymptom(), symptomMsg.getAbout(), symptomMsg.getUniqueIDPipeline(), symptomMsg.getConcernedRequests().toString());
                receivedObsRateSymptoms.putIfAbsent(symptomMsg.getUniqueIDPipeline(), new CircularFifoBuffer(5));
                receivedObsRateSymptoms.get(symptomMsg.getUniqueIDPipeline()).add(new MAPEKSymptomMsgWithDate(symptomMsg));
            }
            // SymptomMsg messages - Virtual Sensors
            else if (symptomMsg.getAbout() == EntityMAPEK.SENSOR) {
                log.info("Received Symptom : {} {} {}", symptomMsg.getSymptom(), symptomMsg.getAbout(), symptomMsg.getConnectedSensors().toString());
                // TODO terminate concerned Requests
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

    private Future<ActorRef> getPlanActor() {
        return getContext().actorSelection(getSelf().path().parent() + "/planActor").resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

    private void tellToPlanActor(RFCMsg rfcMsg) {
        getPlanActor().onComplete(new OnComplete<ActorRef>() {
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
