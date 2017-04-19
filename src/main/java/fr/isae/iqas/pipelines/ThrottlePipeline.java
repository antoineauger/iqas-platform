package fr.isae.iqas.pipelines;

import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.ThrottleMode;
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
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

import static fr.isae.iqas.model.request.Operator.NONE;

/**
 * Created by an.auger on 19/04/2017.
 *
 * ThrottlePipeline is an example of QoO pipeline provided by the iQAS platform.
 * It can be modified according to user needs.
 */
public class ThrottlePipeline extends AbstractPipeline implements IPipeline {
    private Graph runnableGraph = null;

    public ThrottlePipeline() {
        super("Throttle Pipeline", "ThrottlePipeline", true);

        addSupportedOperator(NONE);
        setParameter("obsRate_max", String.valueOf(Integer.MAX_VALUE), true);
    }

    @Override
    public Graph<FlowShape<ConsumerRecord<byte[], String>, ProducerRecord<byte[], String>>, Materializer> getPipelineGraph(String topicToPublish,
                                                                                                                           ObservationLevel askedLevel,
                                                                                                                           Operator operatorToApply) {

        String[] strTab = getParams().get("obsRate_max").split(" ");
        final int nbObsMax = Integer.valueOf(strTab[0]);
        TimeUnit unit = null;
        switch (strTab[1]) {
            case "s":
                unit = TimeUnit.SECONDS;
                break;
            case "min":
                unit = TimeUnit.MINUTES;
                break;
            case "hour":
                unit = TimeUnit.HOURS;
                break;
        }
        TimeUnit finalUnit = unit;

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

                    final FlowShape<RawData, RawData> throttleMechanism = builder.add(
                            Flow.of(RawData.class)
                                    .throttle(nbObsMax, new FiniteDuration(1, finalUnit), 1, ThrottleMode.shaping())
                    );

                    builder.from(consumRecordToRawData.out())
                            .via(throttleMechanism)
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
