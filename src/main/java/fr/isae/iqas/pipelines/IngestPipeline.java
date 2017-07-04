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
import fr.isae.iqas.model.observation.RawData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static fr.isae.iqas.model.request.Operator.NONE;

/**
 * Created by an.auger on 28/02/2017.
 *
 * SensorFilterPipeline is a QoO pipeline provided by the iQAS platform.
 * It should not be modified.
 */
public class IngestPipeline extends AbstractPipeline implements IPipeline {
    private Logger logger = LoggerFactory.getLogger(IngestPipeline.class);

    private Graph runnableGraph = null;

    public IngestPipeline() {
        super("Ingest Pipeline", "IngestPipeline", true);

        setParameter("allowed_sensors", "", true);
        addSupportedOperator(NONE);
    }

    @Override
    public Graph<FlowShape<ConsumerRecord<byte[], String>, ProducerRecord<byte[], String>>, Materializer> getPipelineGraph() {
        runnableGraph = GraphDSL
                .create(builder -> {
                    // Definition of the broadcast for the MAPE-K monitoring
                    //final UniformFanOutShape<RawData, RawData> bcast = builder.add(Broadcast.create(2));

                    final FlowShape<ConsumerRecord, ProducerRecord> testBenchmark = builder.add(
                            Flow.of(ConsumerRecord.class).map(r -> {
                                return new ProducerRecord(
                                        getTopicToPublish(),
                                        r.value());
                            })
                    );

                    /*final FlowShape<ConsumerRecord, RawData> consumRecordToRawData = builder.add(
                            Flow.of(ConsumerRecord.class).map(r -> {
                                JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                long timestamp_now = System.currentTimeMillis();
                                return new RawData(
                                        sensorDataObject.getString("date"),
                                        sensorDataObject.getString("value"),
                                        sensorDataObject.getString("producer"),
                                        sensorDataObject.getString("timestamps"),
                                        "iQAS_in",
                                        timestamp_now);
                            })
                    );

                    final FlowShape<RawData, RawData> filteredInformationBySensor = builder.add(
                            Flow.of(RawData.class).filter(r -> {
                                if (r.getProducer().startsWith("oppSensor_")) {
                                    return true;
                                }
                                String[] allowedSensors = getParams().get("allowed_sensors").split(";");
                                List<String> allowedSensorList = Arrays.asList(allowedSensors);
                                return allowedSensorList.contains(r.getProducer());
                            })
                    );

                    final FlowShape<RawData, ProducerRecord> rawDataToProdRecord = builder.add(
                            Flow.of(RawData.class).map(r -> {
                                ObjectMapper mapper = new ObjectMapper();
                                mapper.enable(SerializationFeature.INDENT_OUTPUT);
                                return new ProducerRecord<byte[], String>(getTopicToPublish(), mapper.writeValueAsString(r));
                            })
                    );*/

                    /*builder.from(testBenchmark.out())
                            //.via(filteredInformationBySensor)
                            //.viaFanOut(bcast)
                            .toInlet(testBenchmark.in());*/

                    // Do not remove - useful for MAPE-K monitoring

                    /*builder.from(bcast)
                            .via(builder.add(Flow.of(RawData.class).map(p -> p.getProducer())))
                            .via(builder.add(getFlowToComputeObsRate()))
                            .to(builder.add(Sink.foreach(elem -> {
                                Map<String, Integer> obsRateByTopic = (Map<String, Integer>) elem;
                                ObsRateReportMsg obsRateReportMsg = new ObsRateReportMsg(getUniqueID());
                                obsRateReportMsg.setObsRateByTopic(obsRateByTopic);
                                getMonitorActor().tell(obsRateReportMsg, ActorRef.noSender());
                            })));*/

                    return new FlowShape<>(testBenchmark.in(), testBenchmark.out());
                });

        return runnableGraph;
    }

    @Override
    public String getPipelineID() {
        return getClass().getSimpleName();
    }

}

