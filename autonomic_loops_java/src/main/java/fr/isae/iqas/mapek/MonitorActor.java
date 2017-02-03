package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaConsumerActor;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import fr.isae.iqas.model.messages.AddKafkaTopicMsg;
import fr.isae.iqas.model.messages.TerminatedMsg;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.concurrent.duration.Duration;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 13/09/2016.
 */

public class MonitorActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private ActorMaterializer materializer = null;
    private ActorRef kafkaActor;
    private ProducerSettings<byte[], String> producerSettings = null;
    private ConsumerSettings<byte[], String> consumerSettings = null;
    private Set<TopicPartition> watchedTopics = new HashSet<>();

    public MonitorActor(Properties prop) {

        consumerSettings = ConsumerSettings.create(getContext().system(), new ByteArrayDeserializer(), new StringDeserializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"))
                .withGroupId("groupInfoMonitor")
                .withClientId("clientInfoMonitor")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        producerSettings = ProducerSettings
                .create(getContext().system(), new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers("localhost:9092");

        materializer = ActorMaterializer.create(getContext().system());

        //TODO check good behaviour, how to properly restart kafka?
        //watchedTopics.add(new TopicPartition("topic1", 0));
        //restartKafkaActor();
    }

    @Override
    public void preStart() {
        /*getContext().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                getSelf(), "tick", getContext().dispatcher(), null);*/
    }

    // override postRestart so we don't call preStart and schedule a new message
    @Override
    public void postRestart(Throwable reason) {
    }

    @Override
    public void onReceive(Object message) {
        if (message.equals("tick")) {
            // send another periodic tick after the specified delay
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(10, TimeUnit.SECONDS),
                    getSelf(), "tick", getContext().dispatcher(), null);

            System.out.println("It works!");
        } else if (message instanceof AddKafkaTopicMsg) {
            log.info("Received AddKafkaTopicMsg message: {}", message);
            getSender().tell(message, getSelf());
            AddKafkaTopicMsg addKafkaTopicMsgMsg = (AddKafkaTopicMsg) message;
            if (!watchedTopics.contains(addKafkaTopicMsgMsg.getTopic())) {
                watchedTopics.add(new TopicPartition(addKafkaTopicMsgMsg.getTopic(), 0));
                restartKafkaActor();
            }
        } else if (message instanceof String) {
            log.info("Received String message: {}", message);
            getSender().tell(message, getSelf());
        } else if (message instanceof TerminatedMsg) {
            TerminatedMsg terminatedMsg = (TerminatedMsg) message;
            if (terminatedMsg.getTargetToStop().path().equals(getSelf().path())) {
                log.info("Received TerminatedMsg message: {}", message);
                if (kafkaActor != null) {
                    getContext().stop(kafkaActor);
                }
                getContext().stop(self());
            }
        } else {
            unhandled(message);
        }
    }

    @Override
    public void postStop() {
    }

    private void restartKafkaActor() {
        // Default KafkaActor for reuse
        if (kafkaActor != null) {
            getContext().stop(kafkaActor);
        }
        kafkaActor = getContext().actorOf(KafkaConsumerActor.props(consumerSettings));

        // Consuming messages with sharing kafka actor

        Consumer.plainExternalSource(kafkaActor, Subscriptions.assignment(watchedTopics))
                .runWith(Sink.foreach(r -> System.out.println("Monitor reading: " + r.toString())), materializer);

        // Other ways without sharing kafkaActor

        /*Consumer.committableSource(consumerSettings, Subscriptions.topics(watchedTopics))
            .map(msg -> filter.filter(msg))
            .runWith(Producer.commitableSink(producerSettings), materializer);*/

        /*Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("topic1"))
            .runWith(Sink.foreach(a -> System.out.println(a)), materializer);*/
    }

}

