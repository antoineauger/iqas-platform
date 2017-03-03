package fr.isae.iqas.mapek;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaConsumerActor;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.pipelines.IPipeline;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.types.ObjectId;
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

    private Source<ConsumerRecord<byte[], String>, Consumer.Control> kafkaSource = null;
    private Sink<ProducerRecord, CompletionStage<Done>> kafkaSink = null;

    private ActorRef kafkaActor = null;

    private ActorMaterializer materializer = null;
    private RunnableGraph myRunnableGraph = null;
    private IPipeline pipelineToEnforce = null;
    private String topicToPublish = null;

    public ExecuteActor(Properties prop, IPipeline pipelineToEnforce, Set<String> topicsToPullFrom, String topicToPublish) {

        String test = new ObjectId().toString();

        ConsumerSettings consumerSettings = ConsumerSettings.create(getContext().system(), new ByteArrayDeserializer(), new StringDeserializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"))
                .withGroupId("group" + test)
                .withClientId("client" + test)
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        ProducerSettings producerSettings = ProducerSettings
                .create(getContext().system(), new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"));

        // Kafka source
        kafkaActor = getContext().actorOf((KafkaConsumerActor.props(consumerSettings)));
        Set<TopicPartition> watchedTopics = new HashSet<>();
        watchedTopics.addAll(topicsToPullFrom.stream().map(s -> new TopicPartition(s, 0)).collect(Collectors.toList()));
        this.kafkaSource = Consumer.plainExternalSource(kafkaActor, Subscriptions.assignment(watchedTopics));

        // Sinks
        this.kafkaSink = Producer.plainSink(producerSettings);

        // Retrieval of available QoO pipelines
        this.pipelineToEnforce = pipelineToEnforce;
        this.topicToPublish = topicToPublish;

        // Materializer to run graphs (QoO pipelines)
        this.materializer = ActorMaterializer.create(getContext().system());
    }

    @Override
    public void preStart() {
        //TODO decide if RD, I or Knowledge!
        myRunnableGraph = RunnableGraph.fromGraph(pipelineToEnforce.getPipelineGraph(
                kafkaSource,
                kafkaSink,
                topicToPublish,
                ObservationLevel.INFORMATION,
                null));
        if (myRunnableGraph != null) {
            myRunnableGraph.run(materializer);
        }
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof TerminatedMsg) {
            TerminatedMsg terminatedMsg = (TerminatedMsg) message;
            if (terminatedMsg.getTargetToStop().path().equals(getSelf().path())) {
                log.info("Received TerminatedMsg message: {}", message);
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
        else if (message instanceof String) {
            log.info("Received String message: {}", message);
        }
    }

    @Override
    public void postStop() {
        // clean up resources here ...
    }

    /*private void askParentForTermination() {
        context.parent().tell(new TerminatedMsg(getSelf()), getSelf());
    }

    private Future<ActorRef> getPipelineWatcherActor() {
        return context.actorSelection(getSelf().path().parent().parent().parent().parent()
                + "/" + "pipelineWatcherActor").resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }*/
}
