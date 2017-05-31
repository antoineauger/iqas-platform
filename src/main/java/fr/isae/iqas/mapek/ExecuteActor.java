package fr.isae.iqas.mapek;

import akka.Done;
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

public class ExecuteActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Source<ConsumerRecord<byte[], String>, Consumer.Control> kafkaSource;
    private Sink<ProducerRecord<byte[], String>, CompletionStage<Done>> kafkaSink;

    private UniqueKillSwitch killSwitch;
    private Pair<Pair<UniqueKillSwitch, Materializer>, CompletionStage<Done>> stream;
    private ActorRef kafkaActor;

    private ActorMaterializer materializer;
    private IPipeline pipelineToEnforce ;

    public ExecuteActor(Properties prop, IPipeline pipelineToEnforce, ObservationLevel askedObsLevel, Set<String> topicsToPullFrom, String topicToPublish) {
        ConsumerSettings consumerSettings = ConsumerSettings.create(getContext().system(), new ByteArrayDeserializer(), new StringDeserializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"))
                .withGroupId("group_" + pipelineToEnforce.getUniqueID())
                .withClientId("client_" + pipelineToEnforce.getUniqueID())
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        ProducerSettings producerSettings = ProducerSettings
                .create(getContext().system(), new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"));

        // Kafka source
        this.kafkaActor = getContext().actorOf((KafkaConsumerActor.props(consumerSettings)));
        Set<TopicPartition> watchedTopics = new HashSet<>();
        watchedTopics.addAll(topicsToPullFrom.stream().map(s -> new TopicPartition(s, 0)).collect(Collectors.toList()));
        this.kafkaSource = Consumer.plainExternalSource(kafkaActor, Subscriptions.assignment(watchedTopics));

        // Sinks
        this.kafkaSink = Producer.plainSink(producerSettings);

        // Mandatory Pipeline configuration
        pipelineToEnforce.setupPipelineGraph(topicToPublish, askedObsLevel, Operator.NONE);
        this.pipelineToEnforce = pipelineToEnforce;

        // Materializer to run graphs (QoO pipelines)
        this.materializer = ActorMaterializer.create(getContext().system());
    }

    @Override
    public void preStart() {
        stream = kafkaSource
                .viaMat(KillSwitches.single(), Keep.right())
                .viaMat(pipelineToEnforce.getPipelineGraph(), Keep.both())
                .toMat(kafkaSink, Keep.both())
                .run(materializer);

        killSwitch = stream.first().first();
    }

    @Override
    public void onReceive(Object message) throws Exception {
        // TerminatedMsg messages
        if (message instanceof TerminatedMsg) {
            TerminatedMsg terminatedMsg = (TerminatedMsg) message;
            if (terminatedMsg.getTargetToStop().path().equals(getSelf().path())) {
                log.info("Received TerminatedMsg message: {}", message);
                if (stream != null) {
                    killSwitch.shutdown();
                }
                if (kafkaActor != null) {
                    log.info("Trying to stop " + kafkaActor.path().name());
                    try {
                        Future<Boolean> stopped = Patterns.gracefulStop(kafkaActor, Duration.create(5, TimeUnit.SECONDS));
                        Await.result(stopped, Duration.create(5, TimeUnit.SECONDS));
                        log.info("Successfully stopped " + kafkaActor.path());
                    } catch (Exception e) {
                        log.error(e.toString());
                    }
                    getContext().stop(kafkaActor);
                }
                getContext().stop(getSelf());
            }
        }
        // IPipeline messages (= live UPDATE of the Pipeline)
        else if (message instanceof IPipeline) {
            // We use the UniqueKillSwitch to stop current stream
            killSwitch.shutdown();

            IPipeline newPipelineToEnforce = (IPipeline) message;
            log.info("Updating pipeline " + newPipelineToEnforce.getUniqueID());

            stream = kafkaSource
                    .viaMat(KillSwitches.single(), Keep.right())
                    .viaMat(newPipelineToEnforce.getPipelineGraph(), Keep.both())
                    .toMat(kafkaSink, Keep.both())
                    .run(materializer);

            killSwitch = stream.first().first();
        }
    }

}
