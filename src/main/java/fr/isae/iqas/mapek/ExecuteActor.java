package fr.isae.iqas.mapek;

import akka.Done;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Pair;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaConsumerActor;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.UniqueKillSwitch;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.request.Operator;
import fr.isae.iqas.pipelines.IPipeline;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by an.auger on 13/09/2016.
 */

public class ExecuteActor extends AbstractActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Source<ConsumerRecord<byte[], String>, Consumer.Control> kafkaSource;
    private Sink<ProducerRecord<byte[], String>, CompletionStage<Done>> kafkaSink;

    private UniqueKillSwitch killSwitch;
    private Pair<Pair<UniqueKillSwitch, Materializer>, CompletionStage<Done>> stream;

    private Materializer materializer;
    private IPipeline pipelineToEnforce ;

    public ExecuteActor(Properties prop, IPipeline pipelineToEnforce,
                        ObservationLevel askedObsLevel,
                        Source<ConsumerRecord<byte[], String>, Consumer.Control> kafkaSource,
                        Sink<ProducerRecord<byte[], String>, CompletionStage<Done>> kafkaSink,
                        Materializer materializer,
                        Set<String> topicsToPullFrom,
                        String topicToPublish) {

        /*ConsumerSettings consumerSettings = ConsumerSettings.create(getContext().system(), new ByteArrayDeserializer(), new StringDeserializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"))
                .withGroupId("group_" + pipelineToEnforce.getUniqueID())
                .withClientId("client_" + pipelineToEnforce.getUniqueID())
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");*/

        /*ProducerSettings producerSettings = ProducerSettings
                .create(getContext().system(), new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"));*/

        // Kafka source
        //this.kafkaActor = getContext().actorOf((KafkaConsumerActor.props(consumerSettings)));
        this.kafkaSource = kafkaSource;
                //Consumer.plainExternalSource(kafkaConsumer, Subscriptions.assignment(watchedTopics));

        // Sinks
        this.kafkaSink = kafkaSink;
        //this.kafkaProducer = kafkaProducer;

        // Mandatory Pipeline configuration
        pipelineToEnforce.setupPipelineGraph(topicToPublish, askedObsLevel, Operator.NONE);
        this.pipelineToEnforce = pipelineToEnforce;

        this.materializer = materializer;

        // Materializer to run graphs (QoO pipelines)
        //this.materializer = ActorMaterializer.create(getContext().system());
    }

    @Override
    public void preStart() {
        stream = kafkaSource.async()
                .viaMat(KillSwitches.single(), Keep.right()).async()
                .viaMat(pipelineToEnforce.getPipelineGraph(), Keep.both()).async()
                .toMat(kafkaSink, Keep.both())
                .run(materializer);

        killSwitch = stream.first().first();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(IPipeline.class, this::actionsOnIPipelineMsg)
                .match(TerminatedMsg.class, this::stopThisActor)
                .build();
    }

    private void actionsOnIPipelineMsg(IPipeline msg) {
        // IPipeline messages (= live UPDATE of the Pipeline)
        // We use the UniqueKillSwitch to stop current stream
        killSwitch.shutdown();

        log.info("Updating pipeline " + msg.getUniqueID());

        stream = kafkaSource.async()
                .viaMat(KillSwitches.single(), Keep.right()).async()
                .viaMat(msg.getPipelineGraph(), Keep.both()).async()
                .toMat(kafkaSink, Keep.both())
                .run(materializer);

        killSwitch = stream.first().first();
    }

    private void stopThisActor(TerminatedMsg msg) {
        if (msg.getTargetToStop().path().equals(getSelf().path())) {
            log.info("Received TerminatedMsg message: {}", msg);
            if (stream != null) {
                killSwitch.shutdown();
            }
            /*if (kafkaActor != null) {
                log.info("Trying to stop " + kafkaActor.path().name());
                try {
                    Future<Boolean> stopped = Patterns.gracefulStop(kafkaActor, Duration.create(5, TimeUnit.SECONDS));
                    Await.result(stopped, Duration.create(5, TimeUnit.SECONDS));
                    log.info("Successfully stopped " + kafkaActor.path());
                } catch (Exception e) {
                    log.error(e.toString());
                }
                getContext().stop(kafkaActor);
            }*/
            getContext().stop(getSelf());
        }
    }

}
