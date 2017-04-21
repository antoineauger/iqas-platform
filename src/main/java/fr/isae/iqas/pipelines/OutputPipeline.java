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
import fr.isae.iqas.model.message.QoOReportMsg;
import fr.isae.iqas.model.observation.Information;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.observation.RawData;
import fr.isae.iqas.model.quality.QoOAttribute;
import fr.isae.iqas.model.request.Operator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static fr.isae.iqas.model.observation.ObservationLevel.*;
import static fr.isae.iqas.model.quality.QoOAttribute.OBS_ACCURACY;
import static fr.isae.iqas.model.quality.QoOAttribute.OBS_FRESHNESS;
import static fr.isae.iqas.model.request.Operator.NONE;

/**
 * Created by an.auger on 28/02/2017.
 *
 * ForwardPipeline is a QoO pipeline provided by the iQAS platform.
 * It should not be modified.
 */
public class OutputPipeline extends AbstractPipeline implements IPipeline {
    private Logger logger = LoggerFactory.getLogger(OutputPipeline.class);
    private Graph runnableGraph = null;

    public OutputPipeline() {
        super("Output Pipeline", "OutputPipeline", false);

        setParameter("age_max", "24 hours", true);
        setParameter("interested_in", "", true);
        addSupportedOperator(NONE);
    }

    @Override
    public Graph<FlowShape<ConsumerRecord<byte[], String>, ProducerRecord<byte[], String>>, Materializer> getPipelineGraph() {
        String[] ageMaxStr = getParams().get("age_max").split(" ");
        long ageLongValue = Long.valueOf(ageMaxStr[0]);
        TimeUnit unit = null;
        switch (ageMaxStr[1]) {
            case "ms":
                unit = TimeUnit.MILLISECONDS;
                break;
            case "s":
                unit = TimeUnit.SECONDS;
                break;
            case "min":
                unit = TimeUnit.MINUTES;
                break;
            case "mins":
                unit = TimeUnit.MINUTES;
                break;
            case "hour":
                unit = TimeUnit.HOURS;
                break;
            case "hours":
                unit = TimeUnit.HOURS;
                break;
        }
        final long ageMaxAllowed = new FiniteDuration(ageLongValue, unit).toMillis();

        final ObservationLevel askedLevelFinal = getAskedLevel();
        runnableGraph = GraphDSL
                .create(builder -> {
                    List<QoOAttribute> interestAttr = new ArrayList<>();
                    if (getParams().get("interested_in") != null){
                        String[] allowedSensors = getParams().get("interested_in").split(";");
                        for (String s : Arrays.asList(allowedSensors)) {
                            interestAttr.add(QoOAttribute.valueOf(s));
                        }
                    }

                    if (askedLevelFinal == RAW_DATA) {
                        final UniformFanOutShape<RawData, RawData> bcast = builder.add(Broadcast.create(2));

                        final FlowShape<ConsumerRecord, RawData> consumRecordToRawData = builder.add(
                                Flow.of(ConsumerRecord.class).map(r -> {
                                    JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                    RawData rawDataTemp = new RawData(
                                            sensorDataObject.getString("date"),
                                            sensorDataObject.getString("value"),
                                            sensorDataObject.getString("producer"),
                                            sensorDataObject.getString("timestamps"),
                                            "iQAS_out",
                                            System.currentTimeMillis());

                                    if (getQooParams().size() > 0) {
                                        if (interestAttr.contains(OBS_ACCURACY)) {
                                            rawDataTemp.setQoOAttribute(OBS_ACCURACY, String.valueOf(getComputeAttributeHelper().computeQoOAccuracy(rawDataTemp, getQooParams())));
                                        }
                                        if (interestAttr.contains(OBS_FRESHNESS)) {
                                            rawDataTemp.setQoOAttribute(OBS_FRESHNESS, String.valueOf(getComputeAttributeHelper().computeQoOFreshness(rawDataTemp)));
                                        }
                                    }

                                    return rawDataTemp;
                                })
                        );

                        final FlowShape<RawData, RawData> removeOutdatedObs = builder.add(
                                Flow.of(RawData.class)
                                        .filter(r -> {
                                            String[] timestampProducedStr = r.getTimestamps().split(";")[0].split(":");
                                            long timestampProduced = Long.valueOf(timestampProducedStr[1]);
                                            /*logger.error("timestampProducedStr: " + timestampProducedStr.toString());
                                            logger.error("timestampProduced: " + String.valueOf(timestampProduced));
                                            logger.error("(System.currentTimeMillis() - timestampProduced): " + String.valueOf((System.currentTimeMillis() - timestampProduced)));
                                            logger.error("result: " + String.valueOf((System.currentTimeMillis() - timestampProduced) < ageMaxAllowed));*/
                                            return (System.currentTimeMillis() - timestampProduced) < ageMaxAllowed;
                                        })
                        );

                        final FlowShape<RawData, ProducerRecord> rawDataToProdRecord = builder.add(
                                Flow.of(RawData.class).map(r -> {
                                    ObjectMapper mapper = new ObjectMapper();
                                    mapper.enable(SerializationFeature.INDENT_OUTPUT);
                                    return new ProducerRecord<byte[], String>(getTopicToPublish(), mapper.writeValueAsString(r));
                                })
                        );

                        builder.from(consumRecordToRawData.out())
                                .via(removeOutdatedObs)
                                .viaFanOut(bcast)
                                .toInlet(rawDataToProdRecord.in());

                        builder.from(bcast)
                                .via(builder.add(Flow.of(RawData.class)
                                        .groupedWithin(Integer.MAX_VALUE, getReportFrequency())
                                        .map(l -> {
                                            Map<String, RawData> relevantInfoForAllProducers = new HashMap<>();
                                            for (RawData i : l) {
                                                relevantInfoForAllProducers.put(i.getProducer(), i);
                                            }
                                            return relevantInfoForAllProducers;
                                        }))
                                )
                                .to(builder.add(Sink.foreach(elem -> {
                                    Map<String, RawData> infoMapTemp = (Map<String, RawData>) elem;
                                    infoMapTemp.forEach((k, v) -> {
                                        final QoOReportMsg qoOReportAttributes = new QoOReportMsg(getUniqueID());
                                        qoOReportAttributes.setProducerName(k);
                                        qoOReportAttributes.setRequestID(getAssociatedRequest_id());
                                        qoOReportAttributes.setQooAttribute(OBS_FRESHNESS.toString(), v.getQoOAttribute(OBS_FRESHNESS));
                                        qoOReportAttributes.setQooAttribute(OBS_ACCURACY.toString(), v.getQoOAttribute(OBS_ACCURACY));
                                        getMonitorActor().tell(qoOReportAttributes, ActorRef.noSender());
                                    });
                                })));

                        return new FlowShape<>(consumRecordToRawData.in(), rawDataToProdRecord.out());
                    }
                    else if (askedLevelFinal == INFORMATION) {
                        final UniformFanOutShape<Information, Information> bcast = builder.add(Broadcast.create(2));

                        final FlowShape<ConsumerRecord, Information> consumRecordToInfo = builder.add(
                                Flow.of(ConsumerRecord.class).map(r -> {
                                    JSONObject sensorDataObject = new JSONObject(r.value().toString());
                                    Information informationTemp =  new Information(
                                            sensorDataObject.getString("date"),
                                            sensorDataObject.getString("value"),
                                            sensorDataObject.getString("producer"),
                                            sensorDataObject.getString("timestamps"),
                                            "iQAS_out",
                                            System.currentTimeMillis());

                                    if (getQooParams().size() > 0) {
                                        if (interestAttr.contains(OBS_ACCURACY)) {
                                            informationTemp.setQoOAttribute(OBS_ACCURACY, String.valueOf(getComputeAttributeHelper().computeQoOAccuracy(informationTemp, getQooParams())));
                                        }
                                        if (interestAttr.contains(OBS_FRESHNESS)) {
                                            informationTemp.setQoOAttribute(OBS_FRESHNESS, String.valueOf(getComputeAttributeHelper().computeQoOFreshness(informationTemp)));
                                        }
                                    }

                                    return informationTemp;
                                })
                        );

                        final FlowShape<Information, Information> removeOutdatedObs = builder.add(
                                Flow.of(Information.class)
                                        .filter(r -> {
                                            String[] timestampProducedStr = r.getTimestamps().split(";")[0].split(":");
                                            long timestampProduced = Long.valueOf(timestampProducedStr[1]);
                                            /*logger.error("timestampProducedStr: " + timestampProducedStr.toString());
                                            logger.error("timestampProduced: " + String.valueOf(timestampProduced));
                                            logger.error("(System.currentTimeMillis() - timestampProduced): " + String.valueOf((System.currentTimeMillis() - timestampProduced)));
                                            logger.error("result: " + String.valueOf((System.currentTimeMillis() - timestampProduced) < ageMaxAllowed));*/
                                            return (System.currentTimeMillis() - timestampProduced) < ageMaxAllowed;
                                        })
                        );

                        final FlowShape<Information, ProducerRecord> infoToProdRecord = builder.add(
                                Flow.of(Information.class).map(r -> {
                                    ObjectMapper mapper = new ObjectMapper();
                                    mapper.enable(SerializationFeature.INDENT_OUTPUT);
                                    return new ProducerRecord<byte[], String>(getTopicToPublish(), mapper.writeValueAsString(r));
                                })
                        );

                        builder.from(consumRecordToInfo.out())
                                .via(removeOutdatedObs)
                                .viaFanOut(bcast)
                                .toInlet(infoToProdRecord.in());

                        builder.from(bcast)
                                .via(builder.add(Flow.of(Information.class)
                                        .groupedWithin(Integer.MAX_VALUE, getReportFrequency())
                                        .map(l -> {
                                            Map<String, Information> relevantInfoForAllProducers = new HashMap<>();
                                            for (Information i : l) {
                                                relevantInfoForAllProducers.put(i.getProducer(), i);
                                            }
                                            return relevantInfoForAllProducers;
                                        }))
                                )
                                .to(builder.add(Sink.foreach(elem -> {
                                    Map<String, Information> infoMapTemp = (Map<String, Information>) elem;
                                    infoMapTemp.forEach((k, v) -> {
                                        final QoOReportMsg qoOReportAttributes = new QoOReportMsg(getUniqueID());
                                        qoOReportAttributes.setProducerName(k);
                                        qoOReportAttributes.setRequestID(getAssociatedRequest_id());
                                        qoOReportAttributes.setQooAttribute(OBS_FRESHNESS.toString(), v.getQoOAttribute(OBS_FRESHNESS));
                                        qoOReportAttributes.setQooAttribute(OBS_ACCURACY.toString(), v.getQoOAttribute(OBS_ACCURACY));
                                        getMonitorActor().tell(qoOReportAttributes, ActorRef.noSender());
                                    });
                                })));

                        return new FlowShape<>(consumRecordToInfo.in(), infoToProdRecord.out());
                    }
                    else if (askedLevelFinal == KNOWLEDGE) {
                        //TODO: code logic for Knowledge for ForwardPipeline
                        return null;
                    }
                    else { // other observation levels are not supported
                        return null;
                    }

                });

        return runnableGraph;
    }

    @Override
    public String getPipelineID() {
        return getClass().getSimpleName();
    }

}

