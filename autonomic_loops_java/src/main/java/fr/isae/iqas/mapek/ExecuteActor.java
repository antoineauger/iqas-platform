package fr.isae.iqas.mapek;

import akka.Done;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.ClosedShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import fr.isae.iqas.model.messages.Terminated;
import fr.isae.iqas.pipelines.IPipeline;
import fr.isae.iqas.pipelines.PipelineClassLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

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

    private Properties prop = null;
    private ActorMaterializer materializer = null;
    private RunnableGraph myRunnableGraph = null;

    private Map<String, IPipeline> pipelines = null;
    private String topicToPushTo = null;

    public ExecuteActor(Properties prop, ActorRef kafkaActor, Set<String> topicsToPullFrom, String topicToPushTo, String remedyToPlan) throws Exception {
        this.producerSettings = ProducerSettings
                .create(getContext().system(), new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers(prop.getProperty("kafka_endpoint_address") + ":" + prop.getProperty("kafka_endpoint_port"));

        // Kafka source
        this.watchedTopics.addAll(topicsToPullFrom.stream().map(s -> new TopicPartition(s, 0)).collect(Collectors.toList()));
        this.kafkaSource = Consumer.plainExternalSource(kafkaActor, Subscriptions.assignment(watchedTopics));

        // Sinks
        this.kafkaSink = Producer.plainSink(producerSettings);
        //this.ignoreSink = Sink.ignore();

        // Retrieval of available QoO pipelines
        this.prop = prop;
        this.topicToPushTo = topicToPushTo;
        this.pipelines = new HashMap<>();

        // Materializer to run graphs (pipelines)
        this.materializer = ActorMaterializer.create(getContext().system());
        loadQoOPipeline(remedyToPlan);
        this.myRunnableGraph = RunnableGraph.fromGraph(buildGraph(remedyToPlan));
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

    private void loadQoOPipeline(String pipelineName) {
        if (!pipelines.containsKey(pipelineName)) {
            File dir = new File((String) prop.get("qoo_pipelines_dir"));
            ClassLoader cl = new PipelineClassLoader((String) prop.get("qoo_pipelines_dir"));
            if (dir.exists() && dir.isDirectory()) {
                String[] files = dir.list();
                for (int i=0 ; i<files.length ; i++) {
                    try {
                        // only consider files ending in ".class" equals to the wanted pipeline
                        if (!files[i].endsWith(".class") && !files[i].equals(pipelineName+".class"))
                            continue;

                        Class aClass = cl.loadClass(files[i].substring(0, files[i].indexOf(".")));
                        log.info("QoO pipeline " + pipelineName + " successfully loaded from bytecode " + pipelineName + ".class!");

                        Class[] intf = aClass.getInterfaces();
                        for (int j=0 ; j<intf.length ; j++) {
                            if (intf[j].getName().equals("fr.isae.iqas.pipelines.IPipeline")) {
                                IPipeline pipelineToLoad = (IPipeline) aClass.newInstance();
                                pipelines.put(pipelineName, pipelineToLoad);
                            }
                        }

                    } catch (Exception e) {
                        log.error("File " + files[i] + " does not contain a valid IPipeline class.");
                        log.error(e.toString());
                    }
                }
            }
        }
        else {
            log.info("QoO pipeline " + pipelineName + " already loaded, skipped import!");
        }
    }

    private Graph<ClosedShape, Materializer> buildGraph(String pipelineName) throws Exception {
        Graph myRunnableGraphToReturn = null;
        try {
            myRunnableGraphToReturn = RunnableGraph.fromGraph(pipelines.get(pipelineName).getPipelineGraph(kafkaSource, kafkaSink, topicToPushTo));
        } catch (Exception e) {
            log.error("Unable to find the QoO pipeline with name " + pipelineName);
            log.error(e.toString());
        }
        return myRunnableGraphToReturn;

        /*Graph myRunnableGraphToReturn = null;
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
        return myRunnableGraphToReturn;*/
    }
}
