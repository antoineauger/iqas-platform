package fr.isae.iqas.mapek.information;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Pair;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaConsumerActor;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.ClosedShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.*;
import fr.isae.iqas.model.messages.AddKafkaTopic;
import fr.isae.iqas.model.messages.Terminated;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static fr.isae.iqas.mechanisms.AvailAdaptMechanisms.*;

/**
 * Created by an.auger on 13/09/2016.
 */

public class MonitorActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private ActorMaterializer materializer = null;
    private ActorRef kafkaActor;
    private ProducerSettings producerSettings = null;
    private ConsumerSettings consumerSettings = null;
    private Set<TopicPartition> watchedTopics = new HashSet<>();

    public MonitorActor(Properties prop) throws Exception {

        consumerSettings = ConsumerSettings.create(getContext().system(), new ByteArrayDeserializer(), new StringDeserializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"))
                .withGroupId("group3")
                .withClientId("client3")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        producerSettings = ProducerSettings
                .create(getContext().system(), new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers("localhost:9092");

        materializer = ActorMaterializer.create(getContext().system());

        // Default KafkaActor for reuse
        //kafkaActor = getContext().system().actorOf(KafkaConsumerActor.props(consumerSettings));
        watchedTopics.add(new TopicPartition("topic1", 0));
        restartKafkaActor();
        //getContext().watch(kafkaActor);
    }

    @Override
    public void preStart() {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                getSelf(), "tick", getContext().dispatcher(), null);
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
        } else if (message instanceof AddKafkaTopic) {
            log.info("Received AddKafkaTopic message: {}", message);
            getSender().tell(message, getSelf());

            AddKafkaTopic addKafkaTopic = (AddKafkaTopic) message;
            String newKafkaTopicToWatch = addKafkaTopic.getTopic();
            if (!watchedTopics.contains(newKafkaTopicToWatch)) {
                watchedTopics.add(new TopicPartition(newKafkaTopicToWatch, 0));
                restartKafkaActor();
            }
        } else if (message instanceof String) {
            log.info("Received String message: {}", message);
            getSender().tell(message, getSelf());
        } else if (message instanceof Terminated) {
            log.info("Received Terminated message: {}", message);
            cleanShutdown();
        } else {
            unhandled(message);
        }
    }

    @Override
    public void postStop() {
        cleanShutdown();
    }

    private void restartKafkaActor() throws Exception {
        if (kafkaActor != null) {
            getContext().system().stop(kafkaActor);
        }
        kafkaActor = getContext().system().actorOf(KafkaConsumerActor.props(consumerSettings));

        // Kafka source
        final Source<ConsumerRecord<byte[], String>, Consumer.Control> kafkaSource = Consumer.plainExternalSource(kafkaActor, Subscriptions.assignment(watchedTopics));

        // Sinks
        Sink<ProducerRecord, CompletionStage<Done>> kafkaSink = Producer.plainSink(producerSettings);
        Sink<ProducerRecord, CompletionStage<Done>> ignoreSink = Sink.ignore();

        //TODO ok to test
        Flow<ConsumerRecord, ProducerRecord, NotUsed> f1 = f_convert_ConsumerToProducer("topic2");
        Flow<ProducerRecord, ProducerRecord, NotUsed> f2 = f_filter_ValuesGreaterThan(3.0);
        Flow<ProducerRecord, ProducerRecord, NotUsed> f3 = f_filter_ValuesLesserThan(3.0);
        Flow<ProducerRecord, ProducerRecord, NotUsed> f4 = f_group_CountBasedMean(3, "topic2");
        Flow<ProducerRecord, ProducerRecord, NotUsed> f5 = f_group_TimeBasedMean(new FiniteDuration(5, TimeUnit.SECONDS), "topic2");


        final RunnableGraph<Pair<CompletionStage<Done>, CompletionStage<Done>>> myRunnableGraph =
                RunnableGraph.fromGraph(
                        GraphDSL
                                .create(kafkaSink, ignoreSink, Keep.both(), (b, kafkaS, ignoreS) -> {
                                    final UniformFanOutShape<ProducerRecord, ProducerRecord> bcast =
                                            b.add(Broadcast.create(2));
                                    b.from(b.add(kafkaSource)).via(b.add(f1)).viaFanOut(bcast);
                                    b.from(bcast).via(b.add(f2)).via(b.add(f5)).to(kafkaS);
                                    b.from(bcast).via(b.add(f3)).to(ignoreS);
                                    return ClosedShape.getInstance();
                                }));

        myRunnableGraph.run(materializer);

        // Other ways without sharing kafkaActor

        /*Consumer.committableSource(consumerSettings, Subscriptions.topics(watchedTopics))
            .map(msg -> filter.filter(msg))
            .runWith(Producer.commitableSink(producerSettings), materializer);*/

        /*Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("topic1"))
            .runWith(Sink.foreach(a -> System.out.println(a)), materializer);*/
    }

    private void cleanShutdown() {
        if (kafkaActor != null) {
            getContext().system().stop(kafkaActor);
        }
    }

}

