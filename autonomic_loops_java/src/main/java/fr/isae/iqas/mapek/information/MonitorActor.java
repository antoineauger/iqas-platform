package fr.isae.iqas.mapek.information;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import fr.isae.iqas.model.events.AddKafkaTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.concurrent.duration.Duration;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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
    private Set<String> watchedTopics =  new HashSet<String>() ;
    private ScriptEngine engine = null;

    public MonitorActor(Properties prop) {

        consumerSettings = ConsumerSettings.create(getContext().system(), new ByteArrayDeserializer(), new StringDeserializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"))
                .withGroupId("group12")
                .withClientId("client12")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        producerSettings = ProducerSettings
                .create(getContext().system(), new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers("localhost:9092");

        materializer = ActorMaterializer.create(getContext().system());

        engine = new ScriptEngineManager().getEngineByName("nashorn");
    }

    @Override
    public void preStart() {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                getSelf(), "tick", getContext().dispatcher(), null);

        // Default KafkaActor for reuse
        kafkaActor = getContext().system().actorOf(KafkaConsumerActor.props(consumerSettings));
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
        getContext().system().stop(kafkaActor);

        kafkaActor = getContext().system().actorOf(KafkaConsumerActor.props(consumerSettings));

        /*for (String topic : watchedTopics) {
            try {
                engine.eval("print('Hello World!');");
            } catch (ScriptException e) {
                e.printStackTrace();
            }


            /*Consumer.plainExternalSource(kafkaActor, Subscriptions.assignment(new TopicPartition(topic, 0)))
                    .runWith(Sink.foreach(a -> System.out.println(a)), materializer);*/
       /* }*/

        Consumer.committableSource(consumerSettings, Subscriptions.topics(watchedTopics))
                .map(msg -> {
                    System.out.println(msg.record().value());
                    return new ProducerMessage.Message<byte[], String, ConsumerMessage.Committable>(
                            new ProducerRecord<>("topic2", msg.record().value()), msg.committableOffset());
                })
                .runWith(Producer.commitableSink(producerSettings), materializer);

        // Other way without sharing kafkaActor
        /*Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("topic1"))
                .runWith(Sink.foreach(a -> System.out.println(a)), materializer);*/
    }

}

