package fr.isae.iqas.pipelines;

import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.observation.RawData;
import fr.isae.iqas.model.request.Operator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import static fr.isae.iqas.model.request.Operator.NONE;

/**
 * Created by an.auger on 19/04/2017.
 *
 * RemoveOutdatedPipeline is an example of QoO pipeline provided by the iQAS platform.
 * It can be modified according to user needs.
 */
public class RemoveOutdatedPipeline extends AbstractPipeline implements IPipeline {
    private Graph runnableGraph = null;

    public RemoveOutdatedPipeline() {
        super("Remove Outdated Pipeline", "RemoveOutdatedPipeline", true);

        addSupportedOperator(NONE);
        setParameter("age_max", String.valueOf(Integer.MAX_VALUE), true);
    }

    @Override
    public Graph<FlowShape<ConsumerRecord<byte[], String>, ProducerRecord<byte[], String>>, Materializer> getPipelineGraph(String topicToPublish,
                                                                                                                           ObservationLevel askedLevel,
                                                                                                                           Operator operatorToApply) {

        runnableGraph = GraphDSL
                .create(builder -> {

                    final FlowShape<ConsumerRecord, RawData> consumRecordToRawData = builder.add(
                            Flow.of(ConsumerRecord.class).map(r -> {
                                JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                return new RawData(
                                        sensorDataObject.getString("date"),
                                        sensorDataObject.getString("value"),
                                        sensorDataObject.getString("producer"),
                                        sensorDataObject.getString("timestamps"));
                            })
                    );

                    final FlowShape<RawData, ProducerRecord> rawDataToProdRecord = builder.add(
                            Flow.of(RawData.class).map(r -> {
                                ObjectMapper mapper = new ObjectMapper();
                                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                                return new ProducerRecord<byte[], String>(topicToPublish, mapper.writeValueAsString(r));
                            })
                    );

                    final FlowShape<RawData, RawData> removeOutdatedObsMechanism = builder.add(
                            Flow.of(RawData.class)
                                    .filter(r -> {
                                        String[] timestampProducedStr = r.getTimestamps().split(",")[0].split(":");
                                        long timestampProduced = Long.valueOf(timestampProducedStr[1]);
                                        return timestampProduced - System.currentTimeMillis() < Long.valueOf(getParams().get("age_max"));
                                    })
                    );

                    builder.from(consumRecordToRawData.out())
                            .via(removeOutdatedObsMechanism)
                            .toInlet(rawDataToProdRecord.in());

                    return new FlowShape<>(consumRecordToRawData.in(), rawDataToProdRecord.out());

                });

        return runnableGraph;
    }

    @Override
    public String getPipelineID() {
        return getClass().getSimpleName();
    }

}
