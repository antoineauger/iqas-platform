package fr.isae.iqas.pipelines;

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
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.observation.RawData;
import fr.isae.iqas.model.request.Operator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static fr.isae.iqas.model.observation.ObservationLevel.*;
import static fr.isae.iqas.model.request.Operator.NONE;

/**
 * Created by an.auger on 28/02/2017.
 *
 * SensorFilterPipeline is a QoO pipeline provided by the iQAS platform.
 * It should not be modified.
 */
public class SensorFilterPipeline extends AbstractPipeline implements IPipeline {
    private Graph runnableGraph = null;

    public SensorFilterPipeline() {
        super("Sensor Filter Pipeline", "SensorFilterPipeline", true);

        setParameter("allowed_sensors", "", true);
        addSupportedOperator(NONE);
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

                    Flow<ConsumerRecord, RawData, NotUsed> consumRecordToInfo =
                            Flow.of(ConsumerRecord.class).map(r -> {
                                JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                return new RawData(
                                        sensorDataObject.getString("timestamp"),
                                        sensorDataObject.getString("value"),
                                        sensorDataObject.getString("producer"));
                            });

                    Flow<RawData, RawData, NotUsed> filteredInformationBySensor =
                            Flow.of(RawData.class).filter(
                                r -> {
                                    String[] allowedSensors = getParams().get("allowed_sensors").split(";");
                                    List<String> allowedSensorList = Arrays.asList(allowedSensors);
                                    return allowedSensorList.contains(r.getProducer());
                                });

                    Flow<RawData, ProducerRecord, NotUsed> infoToProdRecord =
                            Flow.of(RawData.class).map(r -> {
                                ObjectMapper mapper = new ObjectMapper();
                                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                                return new ProducerRecord<byte[], String>(topicToPublish, mapper.writeValueAsString(r));
                            });

                    if (askedLevelFinal == RAW_DATA || askedLevelFinal == INFORMATION || askedLevelFinal == KNOWLEDGE) {
                        builder.from(sourceGraph)
                                .via(builder.add(consumRecordToInfo))
                                .via(builder.add(filteredInformationBySensor))
                                .via(builder.add(infoToProdRecord))
                                .toInlet(sinkGraph);
                    }
                    else { // other observation levels are not supported
                        return null;
                    }

                    // ################################# END OF YOUR CODE #################################

                    return ClosedShape.getInstance();
                });

        return runnableGraph;
    }

    @Override
    public String getPipelineID() {
        return getClass().getSimpleName();
    }

}

