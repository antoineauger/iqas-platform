package fr.isae.iqas.mapek.information;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaConsumerActor;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.pattern.Patterns;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.util.Timeout;
import fr.isae.iqas.model.messages.RFC;
import fr.isae.iqas.model.messages.Terminated;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.concurrent.Await;
import scala.concurrent.Future;
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
public class PlanActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private ActorRef kafkaActor = null;
    private ProducerSettings<byte[], String> producerSettings = null;
    private ConsumerSettings<byte[], String> consumerSettings = null;
    private Source<ConsumerRecord<byte[], String>, Consumer.Control> kafkaSource = null;
    private Sink<ProducerRecord<byte[], String>, CompletionStage<Done>> kafkaSink = null;
    private Sink<ProducerRecord<byte[], String>, CompletionStage<Done>> ignoreSink = null;
    private Set<TopicPartition> watchedTopics = new HashSet<>();

    private String topicToPullFrom = null;
    private String topicToPushTo = null;

    int number = 1;

    public PlanActor(Properties prop, String topicToPullFrom, String topicToPushTo) {
        consumerSettings = ConsumerSettings.create(getContext().system(), new ByteArrayDeserializer(), new StringDeserializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"))
                .withGroupId("groupInfoPlan")
                .withClientId("clientInfoPlan")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        producerSettings = ProducerSettings
                .create(getContext().system(), new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers("localhost:9092");

        this.topicToPullFrom = topicToPullFrom;
        this.topicToPushTo = topicToPushTo;

        // Kafka source
        kafkaActor = getContext().actorOf(KafkaConsumerActor.props(consumerSettings));
        watchedTopics.add(new TopicPartition(topicToPullFrom, 0));
        kafkaSource = Consumer.plainExternalSource(kafkaActor, Subscriptions.assignment(watchedTopics));

        // Sinks
        kafkaSink = Producer.plainSink(producerSettings);
        ignoreSink = Sink.ignore();
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Terminated) {
            log.info("Received Terminated message: {}", message);
            if (kafkaActor != null) {
                getContext().system().stop(kafkaActor);
            }
            getSender().tell(message, getSelf());
            getContext().system().stop(self());
        } else if (message instanceof RFC) {
            log.info("Received RFC message: {}", message);

            RFC receivedRFC = (RFC) message;
            String remedyToPlan = receivedRFC.getRemedyToPlan();
            Graph graphForRemedy = buildGraph(remedyToPlan);

            //TODO new actor name or watch??

            Future<ActorRef> test = getContext().actorSelection(getSelf().path().toString() + "/" + "executeActor" + String.valueOf(number-1)).resolveOne(new Timeout(5, TimeUnit.SECONDS));
            test.onComplete(new OnComplete<ActorRef>() {
                @Override
                public void onComplete(Throwable failure, ActorRef success) throws Throwable {
                    if (failure != null) {
                        log.info("executeActor" + String.valueOf(number-1) + " not started");
                    } else {
                        log.info("executeActor" + String.valueOf(number-1) + " is started");
                        Timeout timeout = new Timeout(Duration.create(15, "seconds"));
                        Future<Object> future = Patterns.ask(success, new Terminated(), timeout);
                        Terminated result = (Terminated) Await.result(future, timeout.duration());
                        log.info("Successfully stopped executeActor" + String.valueOf(number-1));
                    }
                }
            }, getContext().dispatcher());

            getContext().actorOf(Props.create(ExecuteActor.class, graphForRemedy), "executeActor" + String.valueOf(number));
            number += 1;
            getSender().tell(message, getSelf());
        }
    }

    public Graph<ClosedShape, Materializer> buildGraph(String remedyToPlan) throws Exception {
        Graph myRunnableGraphToReturn = null;

        //TODO make dynamic
        if (remedyToPlan.equals("testGraph")) {
            Flow<ConsumerRecord, ProducerRecord, NotUsed> f1 = f_convert_ConsumerToProducer(topicToPushTo);
            Flow<ProducerRecord, ProducerRecord, NotUsed> f2 = f_filter_ValuesGreaterThan(3.0);
            Flow<ProducerRecord, ProducerRecord, NotUsed> f3 = f_filter_ValuesLesserThan(3.0);
            Flow<ProducerRecord, ProducerRecord, NotUsed> f4 = f_group_CountBasedMean(3, topicToPushTo);
            Flow<ProducerRecord, ProducerRecord, NotUsed> f5 = f_group_TimeBasedMean(new FiniteDuration(5, TimeUnit.SECONDS), topicToPushTo);

            myRunnableGraphToReturn = GraphDSL
                    .create(kafkaSink, ignoreSink, Keep.both(), (builder, kafkaS, ignoreS) -> {
                        final UniformFanOutShape bcast = builder.add(Broadcast.create(2));

                        builder.from(builder.add(kafkaSource)).via(builder.add(f1)).viaFanOut(bcast);
                        builder.from(bcast).via(builder.add(f2)).via(builder.add(f5)).to(kafkaS);
                        builder.from(bcast).via(builder.add(f3)).to(ignoreS);
                        return ClosedShape.getInstance();
                    });
        }
        else if (remedyToPlan.equals("none")) {
            Flow<ConsumerRecord, ProducerRecord, NotUsed> f1 = f_convert_ConsumerToProducer(topicToPushTo);

            myRunnableGraphToReturn = GraphDSL
                    .create(ignoreSink, (builder, ignoreS) -> {
                        final UniformFanOutShape bcast = builder.add(Broadcast.create(1));

                        builder.from(builder.add(kafkaSource)).via(builder.add(f1)).viaFanOut(bcast);
                        builder.from(bcast).to(ignoreS);
                        return ClosedShape.getInstance();
                    });
        }

        return myRunnableGraphToReturn;

    }
}
