package fr.isae.iqas.mapek;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorContext;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.Timeout;
import fr.isae.iqas.model.Pipeline;
import fr.isae.iqas.model.messages.PipelineRequestMsg;
import fr.isae.iqas.model.messages.TerminatedMsg;
import fr.isae.iqas.pipelines.IPipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.concurrent.Future;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static akka.pattern.Patterns.ask;

/**
 * Created by an.auger on 13/09/2016.
 */

public class ExecuteActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private UntypedActorContext context;
    private Source<ConsumerRecord<byte[], String>, Consumer.Control> kafkaSource = null;
    private Sink<ProducerRecord, CompletionStage<Done>> kafkaSink = null;

    private ActorMaterializer materializer = null;
    private RunnableGraph myRunnableGraph = null;
    private String remedyToPlan = null;
    private String topicToPublish = null;

    public ExecuteActor(Properties prop, ActorRef kafkaActor, Set<String> topicsToPullFrom, String topicToPublish, String remedyToPlan) throws Exception {
        this.context = getContext();

        ProducerSettings producerSettings = ProducerSettings
                .create(getContext().system(), new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"));

        // Kafka source
        Set<TopicPartition> watchedTopics = new HashSet<>();
        watchedTopics.addAll(topicsToPullFrom.stream().map(s -> new TopicPartition(s, 0)).collect(Collectors.toList()));
        this.kafkaSource = Consumer.plainExternalSource(kafkaActor, Subscriptions.assignment(watchedTopics));

        // Sinks
        this.kafkaSink = Producer.plainSink(producerSettings);

        // Retrieval of available QoO pipelines
        this.remedyToPlan = remedyToPlan;
        this.topicToPublish = topicToPublish;

        // Materializer to run graphs (pipelines)
        this.materializer = ActorMaterializer.create(getContext().system());
    }

    @Override
    public void preStart() {
        getPipelineWatcherActor().onComplete(new OnComplete<ActorRef>() {
            @Override
            public void onComplete(Throwable t, ActorRef pipelineWatcherActor) throws Throwable {
                if (t != null) {
                    log.error("Unable to find the PipelineWatcherActor: " + t.toString());
                } else {
                    ask(pipelineWatcherActor, new PipelineRequestMsg(remedyToPlan), new Timeout(5, TimeUnit.SECONDS)).onComplete(new OnComplete<Object>() {
                        @Override
                        public void onComplete(Throwable t, Object pipelineObject) throws Throwable {
                            if (t != null) {
                                log.error("Unable to find the PipelineWatcherActor: " + t.toString());
                                askParentForTermination();
                            } else {
                                ArrayList<Pipeline> castedResultPipelineObject = (ArrayList<Pipeline>) pipelineObject;
                                if (castedResultPipelineObject.size() == 0) {
                                    log.error("Unable to retrieve the specified QoO pipeline " + remedyToPlan);
                                    askParentForTermination();
                                } else {
                                    IPipeline pipelineToEnforce = castedResultPipelineObject.get(0).getPipelineObject();
                                    myRunnableGraph = RunnableGraph.fromGraph(pipelineToEnforce.getPipelineGraph(kafkaSource, kafkaSink, topicToPublish));
                                    if (myRunnableGraph != null) {
                                        myRunnableGraph.run(materializer);
                                    }
                                }
                            }
                        }
                    }, context.dispatcher());
                }
            }
        }, context.dispatcher());
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof TerminatedMsg) {
            TerminatedMsg terminatedMsg = (TerminatedMsg) message;
            if (terminatedMsg.getTargetToStop().path().equals(getSelf().path())) {
                log.info("Received TerminatedMsg message: {}", message);
                getContext().stop(getSelf());
            }
        }
    }

    @Override
    public void postStop() {
        // clean up resources here ...
    }

    private void askParentForTermination() {
        context.parent().tell(new TerminatedMsg(getSelf()), getSelf());
    }

    private Future<ActorRef> getPipelineWatcherActor() {
        return context.actorSelection(getSelf().path().parent().parent().parent().parent()
                + "/" + "pipelineWatcherActor").resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }
}
