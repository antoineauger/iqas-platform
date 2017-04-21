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
import fr.isae.iqas.pipelines.AbstractPipeline;
import fr.isae.iqas.pipelines.IPipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

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
        super("Custom Pipeline 1", "CustomPipeline1", true);

        addSupportedOperator(NONE);
        setParameter("test_param_1", String.valueOf(Float.MIN_VALUE), true);
    }

    @Override
    public Graph<FlowShape<ConsumerRecord<byte[], String>, ProducerRecord<byte[], String>>, Materializer> getPipelineGraph() {

        final ObservationLevel askedLevelFinal = getAskedLevel();
        runnableGraph = GraphDSL
                .create(builder -> {

                    // ################################# YOUR CODE GOES HERE #################################

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
                                return new ProducerRecord<byte[], String>(getTopicToPublish(), mapper.writeValueAsString(r));
                            })
                    );

                    final FlowShape<RawData, RawData> filteringMechanismRawData = builder.add(
                            Flow.of(RawData.class).filter(r -> r.getValue() < Double.valueOf(getParams().get("test_param_1")))
                    );

                    builder.from(consumRecordToRawData.out())
                            .via(filteringMechanismRawData)
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
