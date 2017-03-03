package fr.isae.iqas.pipelines;

import akka.Done;
import akka.NotUsed;
import akka.kafka.javadsl.Consumer;
import akka.stream.*;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import fr.isae.iqas.model.observation.Information;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.request.Operator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.concurrent.CompletionStage;

import static fr.isae.iqas.model.observation.ObservationLevel.*;
import static fr.isae.iqas.model.request.Operator.NONE;

/**
 * Created by an.auger on 28/02/2017.
 */
public class ForwardPipeline extends AbstractPipeline implements IPipeline {
    private Graph runnableGraph = null;

    public ForwardPipeline() {
        super("Forward Pipeline", "ForwardPipeline", false);

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

                    // Information
                    Flow<ConsumerRecord, Information, NotUsed> consumRecordToInfo =
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

