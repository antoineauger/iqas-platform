import akka.Done;
import akka.NotUsed;
import akka.kafka.javadsl.Consumer;
import akka.stream.*;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import fr.isae.iqas.pipelines.IPipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Created by an.auger on 31/01/2017.
 */
public class SimpleFilteringPipeline implements IPipeline {
    Map<String, String> allParams = new HashMap<>();
    List<String> customizableParams = new ArrayList<>();

    Graph runnableGraph = null;
    static Flow<ConsumerRecord, ConsumerRecord, NotUsed> flowConsumerRecords = Flow.of(ConsumerRecord.class);
    static Flow<ProducerRecord, ProducerRecord, NotUsed> flowProducerRecords = Flow.of(ProducerRecord.class);

    @Override
    public Graph<ClosedShape, Materializer> getPipelineGraph(Source<ConsumerRecord<byte[], String>, Consumer.Control> kafkaSource,
                                                             Sink<ProducerRecord, CompletionStage<Done>> kafkaSink,
                                                             String topicToPublish) {
        runnableGraph = GraphDSL
                .create(builder -> {
                    final Outlet<ConsumerRecord<byte[], String>> sourceGraph = builder.add(kafkaSource).out();
                    final Inlet<ProducerRecord> sinkGraph = builder.add(kafkaSink).in();

                    Flow<ConsumerRecord, ProducerRecord, NotUsed> f1 = flowConsumerRecords.map(r -> new ProducerRecord<byte[], String>(topicToPublish, String.valueOf(r.value())));
                    Flow<ProducerRecord, ProducerRecord, NotUsed> f2 = flowProducerRecords.filter(r -> Float.parseFloat((String) r.value()) < 0.0);

                    builder.from(sourceGraph).via(builder.add(f1)).via(builder.add(f2)).toInlet(sinkGraph);
                    return ClosedShape.getInstance();
                });
        return runnableGraph;
    }

    @Override
    public String getPipelineName() {
        return "Simple Filtering Pipeline";
    }

    @Override
    public boolean isAdaptable() {
        return false;
    }

    @Override
    public Map<String, String> getCurrentParams() {
        return allParams;
    }

    @Override
    public List<String> getCustomizableParams() {
        return customizableParams;
    }

    @Override
    public boolean setParams(Map<String, String> newParams) {
        return false;
    }
}
