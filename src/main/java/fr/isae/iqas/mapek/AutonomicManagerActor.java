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
import fr.isae.iqas.model.message.MAPEKInternalMsg;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.request.Request;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static akka.dispatch.Futures.future;
import static akka.pattern.Patterns.ask;
import static fr.isae.iqas.model.observation.ObservationLevel.RAW_DATA;
import static fr.isae.iqas.model.request.State.Status.*;

/**
 * Created by an.auger on 25/09/2016.
 */

public class AutonomicManagerActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private MongoController mongoController;
    private FusekiController fusekiController;

    private Map<String, Boolean> connectedSensors; // SensorIDs <-> connected (true/false)

    private ActorRef kafkaAdminActor;
    private ActorRef monitorActor;
    private ActorRef analyzeActor;
    private ActorRef planActor;

    public AutonomicManagerActor(Properties prop, ActorRef kafkaAdminActor, MongoController mongoController, FusekiController fusekiController) {
        this.kafkaAdminActor = kafkaAdminActor;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;

        this.connectedSensors = new ConcurrentHashMap<>();

        this.monitorActor = getContext().actorOf(Props.create(MonitorActor.class, prop, mongoController, fusekiController), "monitorActor");
        this.analyzeActor = getContext().actorOf(Props.create(AnalyzeActor.class, prop, mongoController, fusekiController), "analyzeActor");
        this.planActor = getContext().actorOf(Props.create(PlanActor.class, prop, mongoController, fusekiController, kafkaAdminActor), "planActor");
    }

    @Override
    public void preStart() {
        TopicList topicList = fusekiController._findAllTopics();
        for (Topic t : topicList.topics) {
            String topicName = t.topic.split("#")[1];
            TopicEntity topicEntityTemp = new TopicEntity(topicName, RAW_DATA);
            topicEntityTemp.setSource(topicName);

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
        // TerminatedMsg messages
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
                getContext().stop(self());
            }
        }
        // Request messages
        else if (message instanceof Request) {
            Request incomingReq = (Request) message;
            if (incomingReq.getCurrent_status() == CREATED) { // A Request has just been submitted, we check if we can satisfy it
                future(() -> fusekiController._findAllSensorsWithConditions(incomingReq.getLocation(), incomingReq.getTopic()), context().dispatcher())
                        .onComplete(new OnComplete<VirtualSensorList>() {
                            public void onComplete(Throwable throwable, VirtualSensorList virtualSensorList) {
                                if (throwable == null) { // Only continue if there was no error so far
                                    // TODO: uncomment
                                    /*Iterator<VirtualSensor> it = virtualSensorList.sensors.iterator();
                                    while (it.hasNext()) {
                                        VirtualSensor sensor = it.next();
                                        String sensorID = sensor.sensor_id.split("#")[1];
                                        if (!connectedSensors.containsKey(sensorID) || !connectedSensors.get(sensorID)) { // if no info is available or if the sensor is disconnected
                                            it.remove();
                                        }
                                    }*/
                                    if (virtualSensorList.sensors.size() > 0) {
                                        incomingReq.addLog("Found couple (" + incomingReq.getTopic() + " / " + incomingReq.getLocation() + "), forwarding request to Monitor.");
                                        incomingReq.updateState(SUBMITTED);
                                    } else {
                                        incomingReq.addLog("No sensor found for (" + incomingReq.getTopic() + " / " + incomingReq.getLocation() + ").");
                                        incomingReq.updateState(REJECTED);
                                    }
                                    mongoController.updateRequest(incomingReq.getRequest_id(), incomingReq).whenComplete((result, throwable2) -> {
                                        if (result) {
                                            monitorActor.tell(incomingReq, getSelf());
                                        } else {
                                            log.error("Update of the Request " + incomingReq.getRequest_id() + " has failed. " +
                                                    "Not telling anything to Monitor. " + throwable2.toString());
                                        }
                                    });
                                }
                            }
                        }, context().dispatcher());
            }
            else if (incomingReq.getCurrent_status() == REMOVED) {
                monitorActor.tell(incomingReq, getSelf());
            }
            else { // Other cases should raise an error
                log.error("Unknown state for request " + incomingReq.getRequest_id() + " at this stage");
            }
        }
        // SymptomMsg messages [received from Monitor]
        else if (message instanceof MAPEKInternalMsg.SymptomMsg) {
            MAPEKInternalMsg.SymptomMsg symptomMsg = (MAPEKInternalMsg.SymptomMsg) message;
            if (symptomMsg.getSymptom() == MAPEKInternalMsg.SymptomMAPEK.CONNECTION_REPORT && symptomMsg.getAbout() == MAPEKInternalMsg.EntityMAPEK.SENSOR) {
                connectedSensors = symptomMsg.getConnectedSensors();
                log.info("Received Symptom : {} {} {}", symptomMsg.getSymptom(), symptomMsg.getAbout(), connectedSensors.toString());
            }
        }
    }
}
