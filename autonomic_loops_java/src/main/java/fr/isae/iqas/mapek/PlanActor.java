package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaConsumerActor;
import akka.pattern.Patterns;
import fr.isae.iqas.model.message.RFCMsg;
import fr.isae.iqas.model.message.TerminatedMsg;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 13/09/2016.
 */
public class PlanActor extends UntypedActor {
    private String actorNameToResolve = "executeActor";

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private ConsumerSettings consumerSettings = null;
    private ActorRef kafkaActor = null;
    private Properties prop = null;
    private Set<String> topicsToPullFrom = new HashSet<>();
    private String topicToPublish = null;

    private Map<String, ActorRef> execActorsRefs = new HashMap<>();

    public PlanActor(Properties prop) {
        this.prop = prop;
        this.topicsToPullFrom.add("topic1");
        this.topicToPublish = "topic2";

        // We only create one KafkaConsumer actor for all executeActors.
        // TODO: Evaluate performance of this solution
        consumerSettings = ConsumerSettings.create(getContext().system(), new ByteArrayDeserializer(), new StringDeserializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"))
                .withGroupId("groupInfoPlan")
                .withClientId("clientInfoPlan")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    }

    @Override
    public void preStart() {
        kafkaActor = getContext().actorOf((KafkaConsumerActor.props(consumerSettings)));
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof TerminatedMsg) {
            TerminatedMsg terminatedMsg = (TerminatedMsg) message;
            ActorRef actorRefToStop = terminatedMsg.getTargetToStop();
            if (terminatedMsg.getTargetToStop().path().equals(getSelf().path())) {
                log.info("Received TerminatedMsg message: {}", message);
                if (kafkaActor != null) {
                    getContext().stop(kafkaActor);
                }
                getContext().stop(self());
            }
            else {
                gracefulStop(actorRefToStop);
                execActorsRefs.remove(actorNameToResolve);
            }
        } else if (message instanceof RFCMsg) {
            log.info("Received RFCMsg message: {}", message);

            //TODO for now the remedyToPlan is hardcoded!
            RFCMsg receivedRFCMsg = (RFCMsg) message;
            String remedyToPlan = "SimpleFilteringPipeline";

            if (execActorsRefs.containsKey(actorNameToResolve)) { // if reference found, the corresponding actor has been started
                ActorRef actorRefToStop = execActorsRefs.get(actorNameToResolve);
                gracefulStop(actorRefToStop);

                ActorRef actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class, prop, kafkaActor, topicsToPullFrom, topicToPublish, remedyToPlan));
                execActorsRefs.put(actorNameToResolve, actorRefToStart);
                log.info("Successfully started " + actorRefToStart.path());
            } else {
                ActorRef actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class, prop, kafkaActor, topicsToPullFrom, topicToPublish, remedyToPlan));
                execActorsRefs.put(actorNameToResolve, actorRefToStart);
                log.info("Successfully started " + actorRefToStart.path());
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
}
