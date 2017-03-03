package fr.isae.iqas.pipelines;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.kafka.javadsl.Consumer;
import akka.stream.*;
import akka.stream.javadsl.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import fr.isae.iqas.model.message.QoOReportMsg;
import fr.isae.iqas.model.observation.Information;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.request.Operator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static fr.isae.iqas.model.observation.ObservationLevel.*;
import static fr.isae.iqas.model.quality.QoOAttribute.OBS_ACCURACY;
import static fr.isae.iqas.model.quality.QoOAttribute.OBS_FRESHNESS;
import static fr.isae.iqas.model.request.Operator.NONE;

/**
 * Created by an.auger on 03/03/2017.
 */
public class QoOAnnotatorPipeline extends AbstractPipeline implements IPipeline {
    private Graph runnableGraph = null;

    public QoOAnnotatorPipeline() {
        super("QoO Annotator Pipeline", "QoOAnnotatorPipeline", true);

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
                    // Definition of the broadcast for the MAPE-K monitoring
                    final UniformFanOutShape<Information, Information> bcast = builder.add(Broadcast.create(2));


                    // ################################# YOUR CODE GOES HERE #################################

                    Flow<ConsumerRecord, Information, NotUsed> consumRecordToInfo =
                            Flow.of(ConsumerRecord.class).map(r -> {
                                JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                Information tempInformation = new Information(
                                        sensorDataObject.getString("timestamp"),
                                        sensorDataObject.getString("value"),
                                        sensorDataObject.getString("producer"));
                                tempInformation.setQoOAttribute(OBS_ACCURACY, String.valueOf(getComputeAttributeHelper().computeQoOAccuracy(tempInformation, getQooParams())));
                                tempInformation.setQoOAttribute(OBS_FRESHNESS, String.valueOf(getComputeAttributeHelper().computeQoOFreshness(tempInformation)));
                                return tempInformation;
                    });

                    Flow<Information, ProducerRecord, NotUsed> infoToProdRecord =
                            Flow.of(Information.class).map(r -> {
                                ObjectMapper mapper = new ObjectMapper();
                                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                                return new ProducerRecord<byte[], String>(topicToPublish, mapper.writeValueAsString(r));

                            });

                    if (askedLevelFinal == RAW_DATA) {
                        // Annotated observations with QoO attributes cannot be Raw Data!
                        return null;
                    }
                    else if (askedLevelFinal == INFORMATION) {
                        builder.from(sourceGraph)
                                .via(builder.add(consumRecordToInfo))
                                .viaFanOut(bcast)
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
                    builder.from(bcast.out(1))
                            .via(builder.add(Flow.of(Information.class)
                                    .groupedWithin(Integer.MAX_VALUE, getReportFrequency())
                                    .map(l -> {
                                        Map<String, Information> relevantInfoForAllProducers = new HashMap<>();
                                        for (Information i :l) {
                                            relevantInfoForAllProducers.put(i.getProducer(), i);
                                        }
                                        return relevantInfoForAllProducers;
                                    }))
                            )
                            .to(builder.add(Sink.foreach(elem -> {
                                Map<String, Information> infoMapTemp = (Map<String, Information>) elem;
                                infoMapTemp.forEach((k,v) -> {
                                    final QoOReportMsg qoOReportAttributes = new QoOReportMsg(getUniqueID());
                                    qoOReportAttributes.setProducerName(k);
                                    qoOReportAttributes.setRequestID(getAssociatedRequest_id());
                                    qoOReportAttributes.setQooAttribute(OBS_FRESHNESS.toString(), v.getQoOAttribute(OBS_FRESHNESS));
                                    qoOReportAttributes.setQooAttribute(OBS_ACCURACY.toString(), v.getQoOAttribute(OBS_ACCURACY));
                                    getMonitorActor().tell(qoOReportAttributes, ActorRef.noSender());
                                });
                            })));

                    return ClosedShape.getInstance();
                });

        return runnableGraph;
    }

    @Override
    public String getPipelineID() {
        return getClass().getSimpleName();
    }

}
