package fr.isae.iqas.pipelines;

import akka.actor.ActorRef;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Sink;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import fr.isae.iqas.model.message.ObsRateReportMsg;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.observation.RawData;
import fr.isae.iqas.model.request.Operator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static fr.isae.iqas.model.observation.ObservationLevel.*;
import static fr.isae.iqas.model.request.Operator.NONE;

/**
 * Created by an.auger on 03/03/2017.
 *
 * ObsRatePipeline is a QoO pipeline provided by the iQAS platform.
 * It should not be modified.
 */
public class ObsRatePipeline extends AbstractPipeline implements IPipeline {
    private Logger logger = LoggerFactory.getLogger(ObsRatePipeline.class);
    private Graph runnableGraph = null;

    public ObsRatePipeline() {
        super("Obs Rate Pipeline", "ObsRatePipeline", true);

        addSupportedOperator(NONE);
    }

    @Override
    public Graph<FlowShape<ConsumerRecord<byte[], String>, ProducerRecord<byte[], String>>, Materializer> getPipelineGraph(String topicToPublish,
                                                                                                                           ObservationLevel askedLevel,
                                                                                                                           Operator operatorToApply) {

        final ObservationLevel askedLevelFinal = askedLevel;
        runnableGraph = GraphDSL
                .create(builder -> {
                    // Definition of the broadcast for the MAPE-K monitoring
                    final UniformFanOutShape<RawData, RawData> bcast = builder.add(Broadcast.create(2));

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
                                return new ProducerRecord<byte[], String>(topicToPublish, mapper.writeValueAsString(r));
                            })
                    );

                    if (askedLevel == RAW_DATA || askedLevelFinal == INFORMATION || askedLevelFinal == KNOWLEDGE) {
                        builder.from(consumRecordToRawData.out())
                                .viaFanOut(bcast)
                                .toInlet(rawDataToProdRecord.in());
                    }
                    else { // other observation levels are not supported
                        return null;
                    }

                    // ################################# END OF YOUR CODE #################################
                    // Do not remove - useful for MAPE-K monitoring

                    builder.from(bcast)
                            .via(builder.add(Flow.of(RawData.class).map(p -> p.getProducer())))
                            .via(builder.add(getFlowToComputeObsRate()))
                            .to(builder.add(Sink.foreach(elem -> {
                                Map<String, Integer> obsRateByTopic = (Map<String, Integer>) elem;
                                ObsRateReportMsg obsRateReportMsg = new ObsRateReportMsg(getUniqueID());
                                obsRateReportMsg.setObsRateByTopic(obsRateByTopic);
                                getMonitorActor().tell(obsRateReportMsg, ActorRef.noSender());
                            })));

                    return new FlowShape<>(consumRecordToRawData.in(), rawDataToProdRecord.out());
                });

        return runnableGraph;
    }

    @Override
    public String getPipelineID() {
        return getClass().getSimpleName();
    }

}
