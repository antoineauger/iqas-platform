package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.util.Timeout;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.kafka.RequestMappings;
import fr.isae.iqas.kafka.TopicEntity;
import fr.isae.iqas.model.jsonld.Topic;
import fr.isae.iqas.model.jsonld.TopicList;
import fr.isae.iqas.model.jsonld.VirtualSensorList;
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
            log.info("Received Symptom : {} {} {}", symptomMsg.getMsgType(), symptomMsg.getAbout(), symptomMsg.getAttachedObject().toString());

            // Requests
            if (symptomMsg.getAbout() == EntityMAPEK.REQUEST) {
                Request requestTemp = (Request) symptomMsg.getAttachedObject();



                if (requestTemp.getCurrent_status() == State.Status.SUBMITTED) { // Valid Request
                    //TODO: Implement logic

                    mongoController.getAllRequests().whenComplete((result, throwable) -> {
                        if (result.size() > 1) {
                            //TODO: Similarities with already enforced requests -> query database
                            for (Request r : result) {
                                if (!requestTemp.getRequest_id().equals(r.getRequest_id())) {
                                    if (r.equals(requestTemp)) {
                                        log.info("Incoming request " + requestTemp.getRequest_id() + " has same topic/location than " + r.getRequest_id());
                                    }
                                }
                            }
                        }
                        else {
                            getPlanActor().onComplete(new OnComplete<ActorRef>() {
                                @Override
                                public void onComplete(Throwable t, ActorRef planActor) throws Throwable {
                                    if (t != null) {
                                        log.error("Unable to find the PlanActor: " + t.toString());
                                    }
                                    else {
                                        //TODO resolve QoO

                                        VirtualSensorList listTemp = fusekiController._findAllSensorsWithConditions(requestTemp.getLocation(), requestTemp.getTopic());
                                        String tempIDForPipelines = new ObjectId().toString();

                                        RequestMappings requestMappings = new RequestMappings();
                                        requestMappings.addAssociatedRequest(requestTemp);

                                        if (requestTemp.getLocation().equals("ALL") && requestTemp.getTopic().equals("ALL")) { // (Location = ALL, Topic = ALL)
                                            //TODO
                                        }
                                        else if (requestTemp.getTopic().equals("ALL")) { // (Location = x, Topic = ALL)
                                            TopicEntity topicInt = new TopicEntity(requestTemp.getTopic() + "_" + requestTemp.getLocation());
                                            requestMappings.getAllTopics().put(topicInt.getName(), topicInt);

                                            TopicList topicList = fusekiController._findAllTopics();
                                            for (Topic topicObject : topicList.topics) {
                                                String topicName = topicObject.topic.split("#")[1];

                                                TopicEntity topicBase = new TopicEntity(topicName);
                                                topicBase.setSource(topicName);
                                                requestMappings.getAllTopics().put(topicName, topicBase);

                                                requestMappings.addLink(topicBase.getName(), topicInt.getName(), "SensorFilterPipeline_" + tempIDForPipelines);
                                            }

                                            TopicEntity topicObsRate = new TopicEntity(requestTemp.getTopic() + "_" + requestTemp.getLocation() + "_OBSRATE");
                                            requestMappings.getAllTopics().put(topicObsRate.getName(), topicObsRate);

                                            TopicEntity sinkForApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id());
                                            sinkForApp.setSink(requestTemp.getApplication_id());
                                            requestMappings.getAllTopics().put(sinkForApp.getName(), sinkForApp);

                                            TopicEntity sinkQoObeforeApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id() + "_QOO");
                                            requestMappings.getAllTopics().put(sinkQoObeforeApp.getName(), sinkQoObeforeApp);

                                            requestMappings.addLink(topicInt.getName(), topicObsRate.getName(), "ObsRatePipeline_" + tempIDForPipelines);
                                            requestMappings.addLink(topicObsRate.getName(), sinkQoObeforeApp.getName(), "QoOAnnotatorPipeline_" + tempIDForPipelines);
                                            requestMappings.addLink(sinkQoObeforeApp.getName(), sinkForApp.getName(), "ForwardPipeline_" + tempIDForPipelines);

                                        }
                                        else if (requestTemp.getLocation().equals("ALL")) { // (Location = ALL, Topic = x)
                                            TopicEntity topicBase = new TopicEntity(requestTemp.getTopic());
                                            topicBase.setSource(requestTemp.getTopic());
                                            requestMappings.getAllTopics().put(topicBase.getName(), topicBase);

                                            TopicEntity topicObsRate = new TopicEntity(requestTemp.getTopic() + "_ALL" + "_OBSRATE");
                                            requestMappings.getAllTopics().put(topicObsRate.getName(), topicObsRate);

                                            TopicEntity topicInt = new TopicEntity(requestTemp.getTopic() + "_ALL");
                                            requestMappings.getAllTopics().put(topicInt.getName(), topicInt);

                                            TopicEntity sinkForApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id());
                                            sinkForApp.setSink(requestTemp.getApplication_id());
                                            requestMappings.getAllTopics().put(sinkForApp.getName(), sinkForApp);

                                            TopicEntity sinkQoObeforeApp = new TopicEntity(requestTemp.getApplication_id() + "_" + requestTemp.getRequest_id() + "_QOO");
                                            requestMappings.getAllTopics().put(sinkQoObeforeApp.getName(), sinkQoObeforeApp);

                                            requestMappings.addLink(topicBase.getName(), topicInt.getName(), "SensorFilterPipeline_" + tempIDForPipelines);
                                            requestMappings.addLink(topicInt.getName(), topicObsRate.getName(), "ObsRatePipeline_" + tempIDForPipelines);
                                            requestMappings.addLink(topicObsRate.getName(), sinkQoObeforeApp.getName(), "QoOAnnotatorPipeline_" + tempIDForPipelines);
                                            requestMappings.addLink(sinkQoObeforeApp.getName(), sinkForApp.getName(), "ForwardPipeline_" + tempIDForPipelines);
                                        }
                                        else { // (Location = x, Topic = y)
                                            //TODO
                                        }

                                        planActor.tell(new RFCMsg(RFCMAPEK.CREATE, EntityMAPEK.REQUEST, requestTemp, requestMappings), getSelf());
                                    }
                                }
                            }, getContext().dispatcher());
                        }
                    });

                } else if (requestTemp.getCurrent_status() == State.Status.REMOVED) { // Request deleted by the user
                    //TODO: free resources
                }
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

    private Future<ActorRef> getPlanActor() {
        return getContext().actorSelection(getSelf().path().parent()
                + "/" + "planActor").resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }
}
