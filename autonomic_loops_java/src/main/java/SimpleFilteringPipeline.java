import akka.Done;
import akka.NotUsed;
import akka.kafka.javadsl.Consumer;
import akka.stream.*;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import fr.isae.iqas.model.observation.Information;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.request.Operator;
import fr.isae.iqas.pipelines.AbstractPipeline;
import fr.isae.iqas.pipelines.IPipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.util.concurrent.CompletionStage;

import static fr.isae.iqas.model.observation.ObservationLevel.*;
import static fr.isae.iqas.model.request.Operator.NONE;

/**
 * Created by an.auger on 31/01/2017.
 */
public class SimpleFilteringPipeline extends AbstractPipeline implements IPipeline {
    private Graph runnableGraph = null;

    public SimpleFilteringPipeline() {
        super("Simple Filtering Pipeline", "SimpleFilteringPipeline", true);

        addSupportedOperator(NONE);
        setParameter("threshold_min", String.valueOf(Float.MIN_VALUE), true);
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


                    // ################################# YOUR CODE GOES HERE #################################

                    Flow<ConsumerRecord, Information, NotUsed> consumRecordToInfo =
                            Flow.of(ConsumerRecord.class).map(r -> {
                                JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                Information tempInformation = new Information(
                                        sensorDataObject.getString("timestamp"),
                                        sensorDataObject.getString("value"),
                                        sensorDataObject.getString("producer"));
                                return tempInformation;
                    });

                    Flow<Information, Information, NotUsed> filteringMechanism =
                            Flow.of(Information.class).filter(r ->
                            r.getValue() < Double.valueOf(getParams().get("threshold_min")));

                    Flow<Information, ProducerRecord, NotUsed> infoToProdRecord =
                            Flow.of(Information.class).map(r -> {
                                ObjectMapper mapper = new ObjectMapper();
                                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                                return new ProducerRecord<byte[], String>(topicToPublish, mapper.writeValueAsString(r));

                            });

                    if (askedLevelFinal == RAW_DATA) {
                        //TODO: code logic for Raw Data for SimpleFilteringPipeline
                        return null;
                    }
                    else if (askedLevelFinal == INFORMATION) {
                        builder.from(sourceGraph)
                                .via(builder.add(consumRecordToInfo))
                                .via(builder.add(filteringMechanism))
                                .via(builder.add(infoToProdRecord))
                                .toInlet(sinkGraph);
                    }
                    else if (askedLevelFinal == KNOWLEDGE) {
                        //TODO: code logic for Knowledge for SimpleFilteringPipeline
                        return null;
                    }
                    else { // other observation levels are not supported
                        return null;
                    }


                    // ################################# END OF YOUR CODE #################################
                    // Do not remove - useful for MAPE-K monitoring

                    return ClosedShape.getInstance();
                });

        return runnableGraph;
    }

    @Override
    public String getPipelineID() {
        return getClass().getSimpleName();
    }

}
