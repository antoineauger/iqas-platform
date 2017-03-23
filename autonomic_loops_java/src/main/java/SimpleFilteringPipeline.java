import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import fr.isae.iqas.model.observation.Information;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.observation.RawData;
import fr.isae.iqas.model.request.Operator;
import fr.isae.iqas.pipelines.AbstractPipeline;
import fr.isae.iqas.pipelines.IPipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import static fr.isae.iqas.model.observation.ObservationLevel.*;
import static fr.isae.iqas.model.request.Operator.NONE;

/**
 * Created by an.auger on 31/01/2017.
 *
 * SimpleFilteringPipeline is an example of QoO pipeline provided by the iQAS platform.
 * It can be modified according to user needs.
 */
public class SimpleFilteringPipeline extends AbstractPipeline implements IPipeline {
    private Graph runnableGraph = null;

    public SimpleFilteringPipeline() {
        super("Simple Filtering Pipeline", "SimpleFilteringPipeline", true);

        addSupportedOperator(NONE);
        setParameter("threshold_min", String.valueOf(Float.MIN_VALUE), true);
    }

    @Override
    public Graph<FlowShape<ConsumerRecord<byte[], String>, ProducerRecord<byte[], String>>, Materializer> getPipelineGraph(String topicToPublish,
                                                                                                                           ObservationLevel askedLevel,
                                                                                                                           Operator operatorToApply) {

        final ObservationLevel askedLevelFinal = askedLevel;
        runnableGraph = GraphDSL
                .create(builder -> {

                    // ################################# YOUR CODE GOES HERE #################################

                    // Raw Data
                    final FlowShape<ConsumerRecord, RawData> consumRecordToRawData = builder.add(
                            Flow.of(ConsumerRecord.class).map(r -> {
                                JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                return new RawData(
                                        sensorDataObject.getString("timestamp"),
                                        sensorDataObject.getString("value"),
                                        sensorDataObject.getString("producer"));
                            })
                    );

                    final FlowShape<RawData, ProducerRecord> rawDataToProdRecord = builder.add(
                            Flow.of(RawData.class).map(r -> {
                                ObjectMapper mapper = new ObjectMapper();
                                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                                return new ProducerRecord<byte[], String>(topicToPublish, mapper.writeValueAsString(r));
                            })
                    );

                    final FlowShape<RawData, RawData> filteringMechanismRawData = builder.add(
                            Flow.of(RawData.class).filter(r -> r.getValue() < Double.valueOf(getParams().get("threshold_min")))
                    );

                    // Information
                    final FlowShape<ConsumerRecord, Information> consumRecordToInfo = builder.add(
                            Flow.of(ConsumerRecord.class).map(r -> {
                                JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                return new Information(
                                        sensorDataObject.getString("timestamp"),
                                        sensorDataObject.getString("value"),
                                        sensorDataObject.getString("producer"));
                            })
                    );

                    final FlowShape<Information, ProducerRecord> infoToProdRecord = builder.add(
                            Flow.of(Information.class).map(r -> {
                                ObjectMapper mapper = new ObjectMapper();
                                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                                return new ProducerRecord<byte[], String>(topicToPublish, mapper.writeValueAsString(r));
                            })
                    );

                    final FlowShape<Information, Information> filteringMechanismInformation = builder.add(
                            Flow.of(Information.class).filter(r -> r.getValue() < Double.valueOf(getParams().get("threshold_min")))
                    );

                    if (askedLevelFinal == RAW_DATA) {
                        builder.from(consumRecordToRawData.out())
                                .via(filteringMechanismRawData)
                                .toInlet(rawDataToProdRecord.in());

                        return new FlowShape<>(consumRecordToRawData.in(), rawDataToProdRecord.out());
                    }
                    else if (askedLevelFinal == INFORMATION) {
                        builder.from(consumRecordToInfo.out())
                                .via(filteringMechanismInformation)
                                .toInlet(infoToProdRecord.in());

                        return new FlowShape<>(consumRecordToInfo.in(), infoToProdRecord.out());
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

                });

        return runnableGraph;
    }

    @Override
    public String getPipelineID() {
        return getClass().getSimpleName();
    }

}
