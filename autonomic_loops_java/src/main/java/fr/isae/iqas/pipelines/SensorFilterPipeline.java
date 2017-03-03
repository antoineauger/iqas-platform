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
import fr.isae.iqas.model.observation.Information;
import fr.isae.iqas.model.observation.ObservationLevel;
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
 */
public class SensorFilterPipeline extends AbstractPipeline implements IPipeline {
    private Graph runnableGraph = null;

    public SensorFilterPipeline() {
        super("Sensor Filter Pipeline", true);

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

                    // TODO Raw Data

                    Flow<ConsumerRecord, Information, NotUsed> consumRecordToInfo =
                            Flow.of(ConsumerRecord.class).map(r -> {
                                JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                Information tempInformation = new Information(
                                        sensorDataObject.getString("timestamp"),
                                        sensorDataObject.getString("value"),
                                        sensorDataObject.getString("producer"));
                                return tempInformation;
                            });

                    Flow<Information, Information, NotUsed> filteredInformationBySensor =
                            Flow.of(Information.class).filter(
                                r -> {
                                    String[] allowedSensors = getParams().get("allowed_sensors").split(";");
                                    List<String> allowedSensorList = Arrays.asList(allowedSensors);
                                    return allowedSensorList.contains(r.getProducer());
                                });

                    Flow<Information, ProducerRecord, NotUsed> infoToProdRecord =
                            Flow.of(Information.class).map(r -> {
                                ObjectMapper mapper = new ObjectMapper();
                                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                                return new ProducerRecord<byte[], String>(topicToPublish, mapper.writeValueAsString(r));
                            });

                    // TODO Knowledge

                    if (askedLevelFinal == RAW_DATA) {
                        //TODO: code logic for Raw Data for SimpleFilteringPipeline
                        return null;
                    }
                    else if (askedLevelFinal == INFORMATION) {
                        builder.from(sourceGraph)
                                .via(builder.add(consumRecordToInfo))
                                .via(builder.add(filteredInformationBySensor))
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

                    return ClosedShape.getInstance();
                });

        return runnableGraph;
    }

    @Override
    public String getPipelineID() {
        return getClass().getSimpleName();
    }

}

