package fr.isae.iqas.mapek.information;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.*;
import akka.stream.javadsl.*;
import fr.isae.iqas.model.messages.Terminated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static fr.isae.iqas.mechanisms.AvailAdaptMechanisms.*;

/**
 * Created by an.auger on 13/09/2016.
 */
public class ExecuteActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private ProducerSettings producerSettings = null;
    private Source<ConsumerRecord<byte[], String>, Consumer.Control> kafkaSource = null;
    private Sink<ProducerRecord, CompletionStage<Done>> kafkaSink = null;
    private Sink<ProducerRecord, CompletionStage<Done>> ignoreSink = null;
    private Set<TopicPartition> watchedTopics = new HashSet<>();

    private String topicToPushTo = null;

    private ActorMaterializer materializer = null;
    private RunnableGraph myRunnableGraph = null;

    public ExecuteActor(Properties prop, ActorRef kafkaActor, Set<String> topicsToPullFrom, String topicToPushTo, String remedyToPlan) throws Exception {
        producerSettings = ProducerSettings
                .create(getContext().system(), new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"));

        this.topicToPushTo = topicToPushTo;

        // Kafka source
        watchedTopics.addAll(topicsToPullFrom.stream().map(s -> new TopicPartition(s, 0)).collect(Collectors.toList()));
        kafkaSource = Consumer.plainExternalSource(kafkaActor, Subscriptions.assignment(watchedTopics));

        // Sinks
        kafkaSink = Producer.plainSink(producerSettings);
        ignoreSink = Sink.ignore();

        materializer = ActorMaterializer.create(getContext().system());
        myRunnableGraph = RunnableGraph.fromGraph(buildGraph(remedyToPlan));
    }

    @Override
    public void preStart() {
        if (myRunnableGraph != null) {
            myRunnableGraph.run(materializer);
        }
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Terminated) {
            log.info("Received Terminated message: {}", message);
            if (myRunnableGraph != null) {
                materializer.shutdown();
            }
            getContext().stop(self());
        }
    }

    @Override
    public void postStop() {
        // clean up resources here ...
    }

    private Graph<ClosedShape, Materializer> buildGraph(String remedyToPlan) throws Exception {
        Graph myRunnableGraphToReturn = null;

        Flow<ConsumerRecord, ProducerRecord, NotUsed> f1 = f_convert_ConsumerToProducer(topicToPushTo);
        Flow<ProducerRecord, ProducerRecord, NotUsed> f2 = f_filter_ValuesGreaterThan(3.0);
        Flow<ProducerRecord, ProducerRecord, NotUsed> f3 = f_filter_ValuesLesserThan(3.0);
        Flow<ProducerRecord, ProducerRecord, NotUsed> f4 = f_group_CountBasedMean(3, topicToPushTo);
        Flow<ProducerRecord, ProducerRecord, NotUsed> f5 = f_group_TimeBasedMean(new FiniteDuration(5, TimeUnit.SECONDS), topicToPushTo);

        if (remedyToPlan.equals("testGraph")) {
            myRunnableGraphToReturn = GraphDSL
                    .create(builder -> {
                        final Outlet<ConsumerRecord<byte[], String>> sourceGraph = builder.add(kafkaSource).out();
                        final Inlet<ProducerRecord> sinkGraph = builder.add(kafkaSink).in();
                        builder.from(sourceGraph).via(builder.add(f1)).via(builder.add(f2)).toInlet(sinkGraph);
                        return ClosedShape.getInstance();
                    });
        } else if (remedyToPlan.equals("none")) {
            myRunnableGraphToReturn = GraphDSL
                    .create(builder -> {
                        final Outlet<ConsumerRecord<byte[], String>> sourceGraph = builder.add(kafkaSource).out();
                        final Inlet<ProducerRecord> sinkGraph = builder.add(ignoreSink).in();
                        builder.from(sourceGraph).via(builder.add(f1)).toInlet(sinkGraph);
                        return ClosedShape.getInstance();
                    });
        }

        return myRunnableGraphToReturn;
    }
}
