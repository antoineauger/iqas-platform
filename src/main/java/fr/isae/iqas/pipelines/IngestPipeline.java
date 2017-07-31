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
import fr.isae.iqas.pipelines.mechanisms.ObsRateReporter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

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
    private ObjectMapper mapper;

    public IngestPipeline() {
        super("Ingest Pipeline", "IngestPipeline", true);
        setParameter("allowed_sensors", "", true);
        addSupportedOperator(NONE);

        mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @Override
    public Graph<FlowShape<ConsumerRecord<byte[], String>, ProducerRecord<byte[], String>>, Materializer> getPipelineGraph() {
        runnableGraph = GraphDSL
                .create(builder -> {
                    // Definition of the broadcast for the MAPE-K monitoring
                    final UniformFanOutShape<RawData, RawData> bcast = builder.add(Broadcast.create(2));

                    final String[] allowedSensors = getParams().get("allowed_sensors").split(";");
                    final List<String> allowedSensorList = Arrays.asList(allowedSensors);

                    final FlowShape<ConsumerRecord, RawData> consumRecordToRawData = builder.add(
                            Flow.of(ConsumerRecord.class).map(r -> {
                                JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                return new RawData(
                                        sensorDataObject.getString("date"),
                                        sensorDataObject.getString("value"),
                                        sensorDataObject.getString("producer"),
                                        sensorDataObject.getString("timestamps"),
                                        "iQAS_in",
                                        System.currentTimeMillis());
                            })
                    );

                    final FlowShape<RawData, RawData> filteredInformationBySensor = builder.add(
                            Flow.of(RawData.class).filter(r -> r.getProducer().startsWith("open_weather_map_") || r.getProducer().startsWith("oppSensor_") || allowedSensorList.contains(r.getProducer()))
                    );

                    final FlowShape<RawData, ProducerRecord> rawDataToProdRecord = builder.add(
                            Flow.of(RawData.class).map(r -> new ProducerRecord<byte[], String>(getTopicToPublish(), mapper.writeValueAsString(r)))
                    );

                    builder.from(consumRecordToRawData.out())
                            .via(filteredInformationBySensor)
                            .viaFanOut(bcast)
                            .toInlet(rawDataToProdRecord.in());

                    // Do not remove - useful for MAPE-K monitoring

                    builder.from(bcast)
                            .via(builder.add(new ObsRateReporter(getUniqueID(), getReportFrequency())))
                            .to(builder.add(Sink.foreach(elem -> getMonitorActor().tell(new ObsRateReportMsg((ObsRateReportMsg) elem), ActorRef.noSender()))));

                    return new FlowShape<>(consumRecordToRawData.in(), rawDataToProdRecord.out());
                });

        return runnableGraph;
    }

    @Override
    public String getPipelineID() {
        return getClass().getSimpleName();
    }

}

