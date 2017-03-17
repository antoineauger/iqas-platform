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
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.model.request.State;
import org.bson.types.ObjectId;
import scala.concurrent.Future;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static fr.isae.iqas.model.message.MAPEKInternalMsg.*;

/**
 * Created by an.auger on 13/09/2016.
 */
public class AnalyzeActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private static boolean RAW_DATA_ASKED = false;

    private Properties prop;
    private MongoController mongoController;
    private FusekiController fusekiController;

    public AnalyzeActor(Properties prop, MongoController mongoController, FusekiController fusekiController) {
        this.prop = prop;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;
    }

    @Override
    public void onReceive(Object message) throws Exception {
        /**
         * SymptomMsg messages received from MonitorActor
         */
        if (message instanceof SymptomMsg) {
            SymptomMsg symptomMsg = (SymptomMsg) message;
            log.info("Received Symptom : {} {} {}", symptomMsg.getMsgType(), symptomMsg.getAbout(), symptomMsg.getAttachedRequest().toString());

            // Requests
            if (symptomMsg.getAbout() == EntityMAPEK.REQUEST) {
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
                                    TopicEntity fromTopic = similarMappings.getAllTopics().get(fromTopicName);
                                    fromTopic.setSource(fromTopicName);
                                    requestMapping.getAllTopics().put(fromTopic.getName(), fromTopic);

                                    TopicEntity sinkForApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id());
                                    requestMapping.getAllTopics().put(sinkForApp.getName(), sinkForApp);
                                    sinkForApp.setSink(requestTemp.getApplication_id());

                                    // Cleaning retrieved TopicEntity object
                                    fromTopic.getChildren().clear();
                                    fromTopic.getEnforcedPipelines().clear();
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
                                TopicEntity topicInt = new TopicEntity(requestTemp.getTopic() + "_" + requestTemp.getLocation());
                                requestMapping.getAllTopics().put(topicInt.getName(), topicInt);

                                TopicList topicList = fusekiController._findAllTopics();
                                for (Topic topicObject : topicList.topics) {
                                    String topicName = topicObject.topic.split("#")[1];

                                    TopicEntity topicBase = new TopicEntity(topicName);
                                    topicBase.setSource(topicName);
                                    requestMapping.getAllTopics().put(topicName, topicBase);

                                    requestMapping.addLink(topicBase.getName(), topicInt.getName(), "SensorFilterPipeline_" + tempIDForPipelines);
                                }

                                TopicEntity topicObsRate = new TopicEntity(requestTemp.getTopic() + "_" + requestTemp.getLocation() + "_OBSRATE");
                                requestMapping.getAllTopics().put(topicObsRate.getName(), topicObsRate);

                                TopicEntity sinkForApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id());
                                sinkForApp.setSink(requestTemp.getApplication_id());
                                requestMapping.getAllTopics().put(sinkForApp.getName(), sinkForApp);

                                requestMapping.addLink(topicInt.getName(), topicObsRate.getName(), "ObsRatePipeline_" + tempIDForPipelines);

                                if (RAW_DATA_ASKED) {
                                    requestMapping.addLink(topicObsRate.getName(), sinkForApp.getName(), "ForwardPipeline_" + tempIDForPipelines);
                                }
                                else {
                                    TopicEntity sinkQoObeforeApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id() + "_QOO");
                                    requestMapping.getAllTopics().put(sinkQoObeforeApp.getName(), sinkQoObeforeApp);
                                    requestMapping.addLink(topicObsRate.getName(), sinkQoObeforeApp.getName(), "QoOAnnotatorPipeline_" + tempIDForPipelines);
                                    requestMapping.addLink(sinkQoObeforeApp.getName(), sinkForApp.getName(), "ForwardPipeline_" + tempIDForPipelines);
                                }
                            }
                            else { // (Location = x, Topic = y) or (Location = ALL, Topic = x)
                                TopicEntity topicBase = new TopicEntity(requestTemp.getTopic());
                                topicBase.setSource(requestTemp.getTopic());
                                requestMapping.getAllTopics().put(topicBase.getName(), topicBase);

                                TopicEntity topicObsRate = new TopicEntity(requestTemp.getTopic() + "_" + requestTemp.getLocation() + "_OBSRATE");
                                requestMapping.getAllTopics().put(topicObsRate.getName(), topicObsRate);

                                TopicEntity topicInt = new TopicEntity(requestTemp.getTopic() + "_" + requestTemp.getLocation());
                                requestMapping.getAllTopics().put(topicInt.getName(), topicInt);

                                TopicEntity sinkForApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id());
                                sinkForApp.setSink(requestTemp.getApplication_id());
                                requestMapping.getAllTopics().put(sinkForApp.getName(), sinkForApp);

                                requestMapping.addLink(topicBase.getName(), topicInt.getName(), "SensorFilterPipeline_" + tempIDForPipelines);
                                requestMapping.addLink(topicInt.getName(), topicObsRate.getName(), "ObsRatePipeline_" + tempIDForPipelines);

                                if (RAW_DATA_ASKED) {
                                    requestMapping.addLink(topicObsRate.getName(), sinkForApp.getName(), "ForwardPipeline_" + tempIDForPipelines);
                                }
                                else {
                                    TopicEntity sinkQoObeforeApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id() + "_QOO");
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
}
