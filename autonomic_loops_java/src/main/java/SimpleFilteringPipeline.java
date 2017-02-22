import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.kafka.javadsl.Consumer;
import akka.stream.*;
import akka.stream.javadsl.*;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.request.Operator;
import fr.isae.iqas.pipelines.AbstractPipeline;
import fr.isae.iqas.pipelines.IPipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.CompletionStage;

import static fr.isae.iqas.model.observation.ObservationLevel.*;
import static fr.isae.iqas.model.request.Operator.NONE;

/**
 * Created by an.auger on 31/01/2017.
 */
public class SimpleFilteringPipeline extends AbstractPipeline implements IPipeline {
    private Graph runnableGraph = null;
    private static Flow<ConsumerRecord, ConsumerRecord, NotUsed> flowConsumerRecords;
    private static Flow<ProducerRecord, ProducerRecord, NotUsed> flowProducerRecords;

    public SimpleFilteringPipeline() {
        super("Simple Filtering Pipeline", true);

        addSupportedOperator(NONE);
        setParameter("threshold_min", String.valueOf(Float.MIN_VALUE), true);

        flowConsumerRecords = Flow.of(ConsumerRecord.class);
        flowProducerRecords = Flow.of(ProducerRecord.class);
    }

    @Override
    public Graph<ClosedShape, Materializer> getPipelineGraph(Source<ConsumerRecord<byte[], String>, Consumer.Control> kafkaSource,
                                                             Sink<ProducerRecord, CompletionStage<Done>> kafkaSink,
                                                             String topicToPublish,
                                                             ObservationLevel askedLevel,
                                                             Operator operatorToApply) {

        final ObservationLevel askedLevelFinal = askedLevel;
        runnableGraph = GraphDSL
                .create(builder -> {
                    // Definition of kafka topics for Source and Sink
                    final Outlet<ConsumerRecord<byte[], String>> sourceGraph = builder.add(kafkaSource).out();
                    final Inlet<ProducerRecord> sinkGraph = builder.add(kafkaSink).in();
                    // Definition of the broadcast for the MAPE-K monitoring
                    final UniformFanOutShape<ProducerRecord, ProducerRecord> bcast = builder.add(Broadcast.create(2));


                    // ################################# YOUR CODE GOES HERE #################################

                    Flow<ConsumerRecord, ProducerRecord, NotUsed> f1 = flowConsumerRecords.map(r ->
                            new ProducerRecord<byte[], String>(topicToPublish, String.valueOf(r.value())));
                    Flow<ProducerRecord, ProducerRecord, NotUsed> f2 = flowProducerRecords.filter(r ->
                            Float.parseFloat((String) r.value()) < Float.valueOf(getParams().get("threshold_min")));

                    if (askedLevelFinal == RAW_DATA) {
                        builder.from(sourceGraph).via(builder.add(f1)).via(builder.add(f2)).viaFanOut(bcast).toInlet(sinkGraph);
                    }
                    else if (askedLevelFinal == INFORMATION) {
                        builder.from(sourceGraph).via(builder.add(f1)).via(builder.add(f2)).viaFanOut(bcast).toInlet(sinkGraph);
                    }
                    else if (askedLevelFinal == KNOWLEDGE) {
                        builder.from(sourceGraph).via(builder.add(f1)).via(builder.add(f2)).viaFanOut(bcast).toInlet(sinkGraph);
                    }
                    else { // other observation levels are not supported
                        return null;
                    }


                    // ################################# END OF YOUR CODE #################################

                    // Do not remove - useful for MAPE-K monitoring
                    builder.from(bcast)
                            .via(builder.add(Flow.of(ProducerRecord.class).map(p -> "obs")))
                            .via(builder.add(getFlowToComputeObsRate()))
                            .to(builder.add(Sink.foreach(elem -> getMonitorActor().tell(String.valueOf(elem), ActorRef.noSender()))));
                    return ClosedShape.getInstance();
                });

        return runnableGraph;
    }

    @Override
    public String getPipelineID() {
        return getClass().getName();
    }

}
