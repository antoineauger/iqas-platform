package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.util.Timeout;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.kafka.KafkaTopicMsg;
import fr.isae.iqas.kafka.TopicEntity;
import fr.isae.iqas.model.jsonld.Topic;
import fr.isae.iqas.model.jsonld.TopicList;
import fr.isae.iqas.model.jsonld.VirtualSensorList;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.model.request.State;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static akka.pattern.Patterns.ask;

/**
 * Created by an.auger on 25/09/2016.
 */

public class AutoManagerActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private MongoController mongoController;
    private FusekiController fusekiController;

    private Map<String, TopicEntity> allTopics;
    private boolean processingActivated;

    private ActorRef kafkaAdminActor = null;
    private ActorRef monitorActor = null;
    private ActorRef analyzeActor = null;
    private ActorRef planActor = null;
    private ActorRef executeActor = null;

    public AutoManagerActor(Properties prop, ActorRef kafkaAdminActor, MongoController mongoController, FusekiController fusekiController) {
        this.kafkaAdminActor = kafkaAdminActor;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;

        this.monitorActor = getContext().actorOf(Props.create(MonitorActor.class, prop, mongoController, fusekiController), "monitorActor");
        this.analyzeActor = getContext().actorOf(Props.create(AnalyzeActor.class, prop, mongoController, fusekiController), "analyzeActor");
        this.planActor = getContext().actorOf(Props.create(PlanActor.class, prop, mongoController, fusekiController, kafkaAdminActor), "planActor");

        this.allTopics = new ConcurrentHashMap<>();
        this.processingActivated = false;
    }

    @Override
    public void preStart() {
        TopicList topicList = fusekiController._findAllTopics();
        for (Topic t : topicList.topics) {
            String topicName = t.topic.split("#")[1];
            TopicEntity topicEntityTemp = new TopicEntity(topicName);
            topicEntityTemp.setSource(topicName);
            allTopics.put(topicName, topicEntityTemp);

            ask(kafkaAdminActor, new KafkaTopicMsg(KafkaTopicMsg.TopicAction.CREATE, topicName), new Timeout(5, TimeUnit.SECONDS)).onComplete(new OnComplete<Object>() {
                @Override
                public void onComplete(Throwable t, Object booleanResult) throws Throwable {
                    if (t != null) {
                        log.error("Unable to find the KafkaAdminActor: " + t.toString());
                    }
                }
            }, getContext().dispatcher());
        }
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof TerminatedMsg) {
            TerminatedMsg terminatedMsg = (TerminatedMsg) message;
            if (terminatedMsg.getTargetToStop().path().equals(getSelf().path())) {
                log.info("Received TerminatedMsg message: {}", message);
                if (monitorActor != null) {
                    getContext().stop(monitorActor);
                }
                if (analyzeActor != null) {
                    getContext().stop(analyzeActor);
                }
                if (planActor != null) {
                    getContext().stop(planActor);
                }
                if (executeActor != null) {
                    getContext().stop(executeActor);
                }
                getContext().stop(self());
            }
        }
        else if (message instanceof Request) {
            Request incomingReq = (Request) message;

            if (incomingReq.getCurrent_status() == State.Status.CREATED) { // A Request has just been submitted, we check if we can satisfy it
                VirtualSensorList virtualSensorList = fusekiController._findAllSensorsWithConditions(incomingReq.getLocation(), incomingReq.getTopic());
                if (virtualSensorList.sensors.size() > 0) {
                    incomingReq.addLog("Found couple (" + incomingReq.getTopic() + " / " + incomingReq.getLocation() + "), forwarding request to Monitor.");
                    incomingReq.updateState(State.Status.SUBMITTED);
                } else {
                    incomingReq.addLog("No sensor found for (" + incomingReq.getTopic() + " / " + incomingReq.getLocation() + ").");
                    incomingReq.updateState(State.Status.REJECTED);
                }
                mongoController.updateRequest(incomingReq.getRequest_id(), incomingReq).whenComplete((result, throwable) -> {
                    if (result) {
                        monitorActor.tell(incomingReq, getSelf());
                    } else {
                        log.error("Update of the Request " + incomingReq.getRequest_id() + " has failed. " +
                                "Not telling anything to Monitor...");
                    }
                });
            }
            else if (incomingReq.getCurrent_status() == State.Status.REMOVED) {
                monitorActor.tell(incomingReq, getSelf());
            }
            else { // Other cases should raise an error
                log.error("Unknown state for request " + incomingReq.getRequest_id() + " at this stage");
            }

            /*Set<String> topicToPullFrom = new HashSet<>();
            topicToPullFrom.add("topic1");

            if (processingActivated) {
                planActor.tell(
                        new MAPEKInternalMsg.ActionMsg(MAPEKInternalMsg.ActionMAPEK.DELETE,
                                MAPEKInternalMsg.EntityMAPEK.PIPELINE,
                                "SimpleFilteringPipeline",
                                topicToPullFrom,
                                "topic2",
                                requestTemp.getRequestID()
                        ), ActorRef.noSender());
                processingActivated = false;
            }
            else {
                planActor.tell(
                        new MAPEKInternalMsg.ActionMsg(MAPEKInternalMsg.ActionMAPEK.APPLY,
                                MAPEKInternalMsg.EntityMAPEK.PIPELINE,
                                "SimpleFilteringPipeline",
                                topicToPullFrom,
                                "topic2",
                                requestTemp.getRequestID()
                        ), ActorRef.noSender());
                processingActivated = true;
            }*/
        }
    }
}
