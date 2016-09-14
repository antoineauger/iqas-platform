package fr.isae.iqas.mapek.information;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaConsumerActor;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import fr.isae.iqas.mapek.event.AddKafkaTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 13/09/2016.
 */

public class MonitorActor extends UntypedActor {
    LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final ActorMaterializer materializer;
    private final ConsumerSettings<byte[], String> consumerSettings;

    private ActorRef kafkaActor;
    private ArrayList<String> watchedTopics = new ArrayList<>();

    public MonitorActor() {
        this.consumerSettings = ConsumerSettings.create(getContext().system(), new ByteArrayDeserializer(), new StringDeserializer())
                        .withBootstrapServers("localhost:9092")
                        .withGroupId("group1")
                        .withClientId("client1")
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        this.materializer = ActorMaterializer.create(getContext().system());
    }

    @Override
    public void preStart() {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                getSelf(), "tick", getContext().dispatcher(), null);

        // Default KafkaActor for reuse
        this.kafkaActor = getContext().system().actorOf(KafkaConsumerActor.props(consumerSettings));
        //getContext().watch(kafkaActor);
    }

    // override postRestart so we don't call preStart and schedule a new message
    @Override
    public void postRestart(Throwable reason) {
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message.equals("tick")) {
            // send another periodic tick after the specified delay
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(10, TimeUnit.SECONDS),
                    getSelf(), "tick", getContext().dispatcher(), null);

            System.out.println("It works!");
        }
        else if (message instanceof AddKafkaTopic) {
            log.info("Received AddKafkaTopic message: {}", message);
            getSender().tell(message, getSelf());

            AddKafkaTopic addKafkaTopic = (AddKafkaTopic) message;
            String newKafkaTopicToWatch = addKafkaTopic.getTopic();
            if (!watchedTopics.contains(newKafkaTopicToWatch)) {
                watchedTopics.add(newKafkaTopicToWatch);
                restartKafkaActor();
            }
        }
        else if (message instanceof String) {
            log.info("Received String message: {}", message);
            getSender().tell(message, getSelf());
        }
        /*else if (message instanceof Terminated) {
            log.info("Received Terminated message: {}", message);
        }*/
        else {
            unhandled(message);
        }
    }

    @Override
    public void postStop() {
        // TODO: useful ?
        //this.materializer.shutdown();
        //getContext().system().stop(this.kafkaActor);
    }

    private void restartKafkaActor() {
        getContext().system().stop(this.kafkaActor);

        this.kafkaActor = getContext().system().actorOf(KafkaConsumerActor.props(consumerSettings));

        for (String topic : this.watchedTopics) {
            Consumer.plainExternalSource(kafkaActor, Subscriptions.assignment(new TopicPartition(topic, 0)))
                    .runWith(Sink.foreach(a -> System.out.println(a)), materializer);
        }

        // Other way without sharing kafkaActor
        /*Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("topic1"))
                .runWith(Sink.foreach(a -> System.out.println(a)), materializer);*/
    }

}

