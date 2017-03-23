package fr.isae.iqas.pipelines;

import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import fr.isae.iqas.model.observation.Information;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.observation.RawData;
import fr.isae.iqas.model.request.Operator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.util.HashMap;

import static fr.isae.iqas.model.observation.ObservationLevel.*;
import static fr.isae.iqas.model.request.Operator.NONE;

/**
 * Created by an.auger on 28/02/2017.
 *
 * ForwardPipeline is a QoO pipeline provided by the iQAS platform.
 * It should not be modified.
 */
public class ForwardPipeline extends AbstractPipeline implements IPipeline {
    private Graph runnableGraph = null;

    public ForwardPipeline() {
        super("Forward Pipeline", "ForwardPipeline", false);

        addSupportedOperator(NONE);
    }

    @Override
    public Graph<FlowShape<ConsumerRecord<byte[], String>, ProducerRecord<byte[], String>>, Materializer> getPipelineGraph(String topicToPublish,
                                                                                                                           ObservationLevel askedLevel,
                                                                                                                           Operator operatorToApply) {

        final ObservationLevel askedLevelFinal = askedLevel;
        runnableGraph = GraphDSL
                .create(builder -> {

                    // ################################# YOUR CODE GOES HERE #################################

                    if (askedLevelFinal == RAW_DATA) {
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

                        builder.from(consumRecordToRawData.out())
                                .toInlet(rawDataToProdRecord.in());

                        return new FlowShape<>(consumRecordToRawData.in(), rawDataToProdRecord.out());
                    }
                    else if (askedLevelFinal == INFORMATION) {
                        final FlowShape<ConsumerRecord, Information> consumRecordToInfo = builder.add(
                                Flow.of(ConsumerRecord.class).map(r -> {
                                    JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                    Information informationTemp =  new Information(
                                            sensorDataObject.getString("timestamp"),
                                            sensorDataObject.getString("value"),
                                            sensorDataObject.getString("producer"));

                                    ObjectMapper mapper = new ObjectMapper();
                                    TypeReference<HashMap<String,String>> typeRef = new TypeReference<HashMap<String,String>>(){};
                                    HashMap<String,String> qooAttributes = mapper.readValue(sensorDataObject.get("qoOAttributeValues").toString(), typeRef);
                                    if (qooAttributes != null) {
                                        informationTemp.setQoOAttributeValuesFromJSON(qooAttributes);
                                    }

                                    return informationTemp;
                                })
                        );

                        final FlowShape<Information, ProducerRecord> infoToProdRecord = builder.add(
                                Flow.of(Information.class).map(r -> {
                                    ObjectMapper mapper = new ObjectMapper();
                                    mapper.enable(SerializationFeature.INDENT_OUTPUT);
                                    return new ProducerRecord<byte[], String>(topicToPublish, mapper.writeValueAsString(r));
                                })
                        );

                        builder.from(consumRecordToInfo.out())
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

                });

        return runnableGraph;
    }

    @Override
    public String getPipelineID() {
        return getClass().getSimpleName();
    }

}

