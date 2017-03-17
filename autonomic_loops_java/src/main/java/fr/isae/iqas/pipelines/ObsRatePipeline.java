package fr.isae.iqas.pipelines;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.kafka.javadsl.Consumer;
import akka.stream.*;
import akka.stream.javadsl.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import fr.isae.iqas.model.message.ObsRateReportMsg;
import fr.isae.iqas.model.message.QoOReportMsg;
import fr.isae.iqas.model.observation.Information;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.observation.RawData;
import fr.isae.iqas.model.request.Operator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import static fr.isae.iqas.model.observation.ObservationLevel.*;
import static fr.isae.iqas.model.request.Operator.NONE;

/**
 * Created by an.auger on 03/03/2017.
 *
 * ObsRatePipeline is a QoO pipeline provided by the iQAS platform.
 * It should not be modified.
 */
public class ObsRatePipeline extends AbstractPipeline implements IPipeline {
    private Graph runnableGraph = null;

    public ObsRatePipeline() {
        super("Obs Rate Pipeline", "ObsRatePipeline", true);

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
                    final UniformFanOutShape<RawData, RawData> bcast = builder.add(Broadcast.create(2));

                    // ################################# YOUR CODE GOES HERE #################################

                    Flow<ConsumerRecord, RawData, NotUsed> consumRecordToInfo =
                            Flow.of(ConsumerRecord.class).map(r -> {
                                JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                return (RawData) new Information(
                                        sensorDataObject.getString("timestamp"),
                                        sensorDataObject.getString("value"),
                                        sensorDataObject.getString("producer"));
                    });

                    Flow<RawData, ProducerRecord, NotUsed> infoToProdRecord =
                            Flow.of(RawData.class).map(r -> {
                                ObjectMapper mapper = new ObjectMapper();
                                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                                return new ProducerRecord<byte[], String>(topicToPublish, mapper.writeValueAsString(r));

                            });

                    if (askedLevel == RAW_DATA || askedLevelFinal == INFORMATION || askedLevelFinal == KNOWLEDGE) {
                        builder.from(sourceGraph)
                                .via(builder.add(consumRecordToInfo))
                                .viaFanOut(bcast)
                                .via(builder.add(infoToProdRecord))
                                .toInlet(sinkGraph);
                    }
                    else { // other observation levels are not supported
                        return null;
                    }


                    // ################################# END OF YOUR CODE #################################
                    // Do not remove - useful for MAPE-K monitoring
                    final ObsRateReportMsg obsRateReportMsg = new ObsRateReportMsg(getUniqueID());
                        builder.from(bcast.out(1))
                                .via(builder.add(Flow.of(RawData.class).map(p -> p.getProducer())))
                                .via(builder.add(getFlowToComputeObsRate()))
                                .to(builder.add(Sink.foreach(elem -> {
                                    Map<String, Integer> obsRateByTopic = (Map<String, Integer>) elem;
                                    obsRateReportMsg.setObsRateByTopic(obsRateByTopic);
                                    getMonitorActor().tell(obsRateReportMsg, ActorRef.noSender());
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
