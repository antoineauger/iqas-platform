package fr.isae.iqas.mapek;

import akka.Done;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaConsumerActor;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.util.Timeout;
import fr.isae.iqas.config.Config;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.kafka.KafkaTopicMsg;
import fr.isae.iqas.kafka.TopicEntity;
import fr.isae.iqas.model.jsonld.Topic;
import fr.isae.iqas.model.jsonld.TopicList;
import fr.isae.iqas.model.jsonld.VirtualSensorList;
import fr.isae.iqas.model.message.SymptomMsg;
import fr.isae.iqas.model.message.SymptomMsgConnectionReport;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.request.Request;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static akka.dispatch.Futures.future;
import static akka.pattern.Patterns.ask;
import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK.SENSOR;
import static fr.isae.iqas.model.message.MAPEKenums.SymptomMAPEK.CONNECTION_REPORT;
import static fr.isae.iqas.model.message.MAPEKenums.SymptomMAPEK.UPDATED;
import static fr.isae.iqas.model.observation.ObservationLevel.RAW_DATA;
import static fr.isae.iqas.model.request.State.Status.*;

/**
 * Created by an.auger on 25/09/2016.
 */

public class AutonomicManagerActor extends AbstractActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private MongoController mongoController;
    private FusekiController fusekiController;

    private Map<String, Boolean> connectedSensors; // SensorIDs <-> connected (true/false)

    private ActorRef kafkaAdminActor;
    private ActorRef monitorActor;
    private ActorRef analyzeActor;
    private ActorRef planActor;

    public AutonomicManagerActor(Config iqasConfig, ActorSystem system, Materializer materializer, ActorRef kafkaAdminActor, MongoController mongoController, FusekiController fusekiController) {
        Properties prop = iqasConfig.getProp();

        this.kafkaAdminActor = kafkaAdminActor;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;

        this.connectedSensors = new ConcurrentHashMap<>();

        ConsumerSettings consumerSettings = ConsumerSettings.create(system, new ByteArrayDeserializer(), new StringDeserializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"))
                .withGroupId("groupAutoManager")
                .withClientId("clientAutoManager")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        ProducerSettings producerSettings = ProducerSettings
                .create(system, new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"));

        ActorRef kafkaConsumer = system.actorOf((KafkaConsumerActor.props(consumerSettings)));
        KafkaProducer<byte[], String> kafkaProducer = producerSettings.createKafkaProducer();
        //Sink<ProducerRecord<byte[], String>, CompletionStage<Done>> kafkaSink = Producer.plainSink(producerSettings, kafkaProducer);

        this.monitorActor = getContext().actorOf(Props.create(MonitorActor.class, prop, mongoController, fusekiController), "monitorActor");
        this.analyzeActor = getContext().actorOf(Props.create(AnalyzeActor.class, prop, mongoController, fusekiController), "analyzeActor");
        this.planActor = getContext().actorOf(Props.create(PlanActor.class, iqasConfig, mongoController, fusekiController, kafkaAdminActor, kafkaConsumer, kafkaProducer, materializer), "planActor");
    }

    @Override
    public void preStart() {
        future(() -> fusekiController.findAllTopics(), context().dispatcher())
                .onComplete(new OnComplete<TopicList>() {
                    public void onComplete(Throwable throwable, TopicList topicListResult) {
                        if (throwable == null) { // Only continue if there was no error so far
                            for (Topic t : topicListResult.topics) {
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
                    }
                }, context().dispatcher());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TerminatedMsg.class, this::stopThisActor)
                .match(Request.class, this::actionsOnRequestMsg)
                .match(SymptomMsgConnectionReport.class, this::actionsOnSymptomMsgConnectionReportMsg)
                .match(SymptomMsg.class, this::actionsOnSymptomMsg)
                .build();
    }

    private void stopThisActor(TerminatedMsg msg) {
        if (msg.getTargetToStop().path().equals(getSelf().path())) {
            log.info("Received TerminatedMsg message: {}", msg);
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

    private void actionsOnRequestMsg(Request msg) {
        if (msg.getCurrent_status() == CREATED) { // A Request has just been submitted, we check if we can satisfy it
            future(() -> fusekiController.findAllSensorsWithConditions(msg.getLocation(), msg.getTopic()), context().dispatcher())
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
                                    msg.addLog("Found couple (" + msg.getTopic() + " / " + msg.getLocation() + "), request forwarded to Monitor.");
                                    msg.updateState(SUBMITTED);
                                } else {
                                    msg.addLog("No sensor found for (" + msg.getTopic() + " / " + msg.getLocation() + ").");
                                    msg.updateState(REJECTED);
                                }
                                mongoController.updateRequest(msg.getRequest_id(), msg).whenComplete((result, throwable2) -> {
                                    if (result) {
                                        monitorActor.tell(msg, getSelf());
                                    } else {
                                        log.error("Update of the Request " + msg.getRequest_id() + " has failed. " +
                                                "Not telling anything to Monitor. " + throwable2.toString());
                                    }
                                });
                            }
                        }
                    }, context().dispatcher());
        }
        else if (msg.getCurrent_status() == REMOVED) {
            monitorActor.tell(msg, getSelf());
        }
        else { // Other cases should raise an error
            log.error("Unknown state for request " + msg.getRequest_id() + " at this stage");
        }
    }

    private void actionsOnSymptomMsgConnectionReportMsg(SymptomMsgConnectionReport msg) {
        // SymptomMsg messages [received from Monitor]
        if (msg.getSymptom() == CONNECTION_REPORT && msg.getAbout() == SENSOR) {
            connectedSensors = msg.getConnectedSensors();
            log.info("Received Symptom : {} {} {}", msg.getSymptom(), msg.getAbout(), connectedSensors.toString());
        }
    }

    private void actionsOnSymptomMsg(SymptomMsg msg) {
        // Sensor UPDATE in Fuseki
        if (msg.getSymptom() == UPDATED && msg.getAbout() == SENSOR) {
            monitorActor.tell(new SymptomMsg(UPDATED, SENSOR), getSelf());
        }
    }
}
