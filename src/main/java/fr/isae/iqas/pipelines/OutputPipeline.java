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
import fr.isae.iqas.config.Config;
import fr.isae.iqas.model.jsonld.VirtualSensor;
import fr.isae.iqas.model.jsonld.VirtualSensorList;
import fr.isae.iqas.model.message.QoOReportMsg;
import fr.isae.iqas.model.observation.Information;
import fr.isae.iqas.model.observation.Knowledge;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.observation.RawData;
import fr.isae.iqas.model.quality.MySpecificQoOAttributeComputation;
import fr.isae.iqas.model.quality.QoOAttribute;
import fr.isae.iqas.pipelines.mechanisms.QoOReporter;
import fr.isae.iqas.utils.JenaUtils;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.rdf.model.Property;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    private Map<String, VirtualSensor> allVirtualSensors; // <sensor_id, VirtualSensor>
    private Map<String, String> pref; // <Abbreviated prefix (e.g. qoo), Full ontology namespace (with #)>
    private Map<String, OntClass> qooC; // <Abbreviated OntClass identifier (e.g. ssn:Sensor), OntClass>
    private Map<String, Property> qooP; // <Abbreviated Property identifier (e.g. qoo:hasQoO), Property>
    private OntModel qooBaseModel;

    private ObjectMapper mapper;

    public OutputPipeline() {
        super("Output Pipeline", "OutputPipeline", false);

        setParameter("age_max", "unset", true);
        setParameter("interested_in", "", true);
        addSupportedOperator(NONE);

        this.allVirtualSensors = new ConcurrentHashMap<>();

        mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public void setSensorContext(Config iqasConfig, VirtualSensorList virtualSensorList, OntModel qooBaseModel) {
        this.qooBaseModel = qooBaseModel;
        this.allVirtualSensors.clear();
        for (VirtualSensor v : virtualSensorList.sensors) {
            String sensorID = v.sensor_id.split("#")[1];
            this.allVirtualSensors.put(sensorID, v);
        }
        this.pref = JenaUtils.getPrefixes(iqasConfig);
        this.qooC = JenaUtils.getUsefulOntClasses(iqasConfig, qooBaseModel);
        this.qooP = JenaUtils.getUsefulProperties(iqasConfig, qooBaseModel);
    }

    public void copyConfiguration(OutputPipeline outputPipelineToMimic) {
        if (outputPipelineToMimic.getParams().containsKey("interested_in")) {
            this.setCustomizableParameter("interested_in", outputPipelineToMimic.getParams().get("interested_in"));
        }
        if (outputPipelineToMimic.getParams().containsKey("age_max")) {
            this.setCustomizableParameter("age_max", outputPipelineToMimic.getParams().get("age_max"));
        }

        this.setAssociatedRequestID(outputPipelineToMimic.getAssociatedRequest_id());
        this.setTempID(outputPipelineToMimic.getTempID());
        this.setOptionsForMAPEKReporting(outputPipelineToMimic.getMonitorActor(), outputPipelineToMimic.getReportFrequency());
        this.setOptionsForQoOComputation(new MySpecificQoOAttributeComputation(), outputPipelineToMimic.getQooParams());

        this.qooBaseModel = outputPipelineToMimic.qooBaseModel;
        this.allVirtualSensors.clear();
        outputPipelineToMimic.allVirtualSensors.forEach((k, v) -> {
            this.allVirtualSensors.put(k, v);
        });
        this.pref = outputPipelineToMimic.pref;
        this.qooC = outputPipelineToMimic.qooC;
        this.qooP = outputPipelineToMimic.qooP;
    }

    @Override
    public Graph<FlowShape<ConsumerRecord<byte[], String>, ProducerRecord<byte[], String>>, Materializer> getPipelineGraph() {
        TimeUnit unit = null;
        long ageLongValue;
        if (!getParams().get("age_max").equals("unset")) {
            String[] ageMaxStr = getParams().get("age_max").split(" ");
            ageLongValue = Long.valueOf(ageMaxStr[0]);
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
        }
        else {
            unit = TimeUnit.HOURS;
            ageLongValue = 24;
        }
        final long ageMaxAllowed = new FiniteDuration(ageLongValue, unit).toMillis();

        final ObservationLevel askedLevelFinal = getAskedLevel();

        List<QoOAttribute> interestAttr = new ArrayList<>();
        if (getParams().get("interested_in") != null) {
            String[] allowedSensors = getParams().get("interested_in").split(";");
            for (String s : Arrays.asList(allowedSensors)) {
                interestAttr.add(QoOAttribute.valueOf(s));
            }
        }

        Graph runnableGraph = GraphDSL.create(builder -> {

            if (askedLevelFinal == RAW_DATA) {
                final UniformFanOutShape<RawData, RawData> bcast = builder.add(Broadcast.create(2));

                final FlowShape<ConsumerRecord, RawData> consumRecordToRawData = builder.add(
                        Flow.of(ConsumerRecord.class).map(r -> {
                            JSONObject sensorDataObject = new JSONObject(r.value().toString());
                            RawData rawDataTemp = new RawData(
                                    sensorDataObject.getString("date"),
                                    sensorDataObject.getString("value"),
                                    sensorDataObject.getString("producer"),
                                    sensorDataObject.getString("timestamps"));

                            if (getQooParams().size() > 0) {
                                if (interestAttr.contains(OBS_ACCURACY)) {
                                    rawDataTemp.setQoOAttribute(OBS_ACCURACY,
                                            String.valueOf(getComputeAttributeHelper().computeQoOAccuracy(rawDataTemp, getQooParams())));
                                }
                                if (interestAttr.contains(OBS_FRESHNESS)) {
                                    rawDataTemp.setQoOAttribute(OBS_FRESHNESS,
                                            String.valueOf(getComputeAttributeHelper().computeQoOFreshness(rawDataTemp)));
                                }
                            }

                            return rawDataTemp;
                        })
                );

                final FlowShape<RawData, RawData> removeOutdatedObs = builder.add(
                        Flow.of(RawData.class).filter(r -> {
                            String[] timestampProducedStr = r.getTimestamps().split(";")[0].split(":");
                            return getParams().get("age_max").equals("unset") || (System.currentTimeMillis() - Long.valueOf(timestampProducedStr[1])) < ageMaxAllowed;
                        })
                );

                final FlowShape<RawData, ProducerRecord> rawDataToProdRecord = builder.add(
                        Flow.of(RawData.class).map(r -> {
                            r.addTimestamp("iQAS_out", System.currentTimeMillis());
                            return new ProducerRecord<byte[], String>(getTopicToPublish(), mapper.writeValueAsString(r));
                        })
                );

                builder.from(consumRecordToRawData.out())
                        .via(removeOutdatedObs)
                        .viaFanOut(bcast)
                        .toInlet(rawDataToProdRecord.in());

                builder.from(bcast)
                        .via(builder.add(new QoOReporter(getUniqueID(), getAssociatedRequest_id())))
                        .to(builder.add(Sink.foreach(elem -> getMonitorActor().tell(new QoOReportMsg((QoOReportMsg) elem), ActorRef.noSender()))));

                return new FlowShape<>(consumRecordToRawData.in(), rawDataToProdRecord.out());

            } else if (askedLevelFinal == INFORMATION) {
                final UniformFanOutShape<Information, Information> bcast = builder.add(Broadcast.create(2));

                final FlowShape<ConsumerRecord, Information> consumRecordToInfo = builder.add(
                        Flow.of(ConsumerRecord.class).map(r -> {
                            JSONObject sensorDataObject = new JSONObject(r.value().toString());
                            String producer = sensorDataObject.getString("producer");
                            Information informationTemp = new Information(
                                    sensorDataObject.getString("date"),
                                    sensorDataObject.getString("value"),
                                    producer,
                                    sensorDataObject.getString("timestamps"));

                            if (getQooParams().size() > 0) {
                                if (interestAttr.contains(OBS_ACCURACY)) {
                                    informationTemp.setQoOAttribute(OBS_ACCURACY,
                                            String.valueOf(getComputeAttributeHelper().computeQoOAccuracy(informationTemp, getQooParams())));
                                }
                                if (interestAttr.contains(OBS_FRESHNESS)) {
                                    informationTemp.setQoOAttribute(OBS_FRESHNESS,
                                            String.valueOf(getComputeAttributeHelper().computeQoOFreshness(informationTemp)));
                                }
                            }

                            if (allVirtualSensors.containsKey(producer)) {
                                informationTemp.setContext(allVirtualSensors.get(producer));
                            }

                            return informationTemp;
                        })
                );

                final FlowShape<Information, Information> removeOutdatedObs = builder.add(
                        Flow.of(Information.class).filter(r -> {
                            String[] timestampProducedStr = r.getTimestamps().split(";")[0].split(":");
                            return getParams().get("age_max").equals("unset") || (System.currentTimeMillis() - Long.valueOf(timestampProducedStr[1])) < ageMaxAllowed;
                        })
                );

                final FlowShape<Information, ProducerRecord> infoToProdRecord = builder.add(
                        Flow.of(Information.class).map(r -> {
                            r.addTimestamp("iQAS_out", System.currentTimeMillis());
                            return new ProducerRecord<byte[], String>(getTopicToPublish(), mapper.writeValueAsString(r));
                        })
                );

                builder.from(consumRecordToInfo.out())
                        .via(removeOutdatedObs)
                        .viaFanOut(bcast)
                        .toInlet(infoToProdRecord.in());

                builder.from(bcast)
                        .via(builder.add(new QoOReporter(getUniqueID(), getAssociatedRequest_id())))
                        .to(builder.add(Sink.foreach(elem -> getMonitorActor().tell(new QoOReportMsg((QoOReportMsg) elem), ActorRef.noSender()))));

                return new FlowShape<>(consumRecordToInfo.in(), infoToProdRecord.out());

            }
            else if (askedLevelFinal == KNOWLEDGE) {
                final UniformFanOutShape<RawData, RawData> bcast = builder.add(Broadcast.create(2));

                final FlowShape<ConsumerRecord, RawData> consumRecordToRawData = builder.add(
                        Flow.of(ConsumerRecord.class).map(r -> {
                            JSONObject sensorDataObject = new JSONObject(r.value().toString());
                            RawData rawDataTemp = new RawData(
                                    sensorDataObject.getString("date"),
                                    sensorDataObject.getString("value"),
                                    sensorDataObject.getString("producer"),
                                    sensorDataObject.getString("timestamps"));

                            if (getQooParams().size() > 0) {
                                if (interestAttr.contains(OBS_ACCURACY)) {
                                    rawDataTemp.setQoOAttribute(OBS_ACCURACY,
                                            String.valueOf(getComputeAttributeHelper().computeQoOAccuracy(rawDataTemp, getQooParams())));
                                }
                                if (interestAttr.contains(OBS_FRESHNESS)) {
                                    rawDataTemp.setQoOAttribute(OBS_FRESHNESS,
                                            String.valueOf(getComputeAttributeHelper().computeQoOFreshness(rawDataTemp)));
                                }
                            }

                            return rawDataTemp;
                        })
                );

                final FlowShape<RawData, RawData> removeOutdatedObs = builder.add(
                        Flow.of(RawData.class).filter(r -> {
                            String[] timestampProducedStr = r.getTimestamps().split(";")[0].split(":");
                            return getParams().get("age_max").equals("unset") || (System.currentTimeMillis() - Long.valueOf(timestampProducedStr[1])) < ageMaxAllowed;
                        })
                );

                final FlowShape<RawData, ProducerRecord> rawDataToProdRecord = builder.add(
                        Flow.of(RawData.class).map(rawDataTemp -> {

                            Knowledge knowledgeTemp = new Knowledge(
                                    rawDataTemp,
                                    allVirtualSensors.get(rawDataTemp.getProducer()),
                                    pref,
                                    qooC,
                                    qooP,
                                    qooBaseModel);

                            if (getQooParams().size() > 0) {
                                if (interestAttr.contains(OBS_ACCURACY)) {
                                    knowledgeTemp.setQoOAttribute(OBS_ACCURACY, rawDataTemp.getQoOAttribute(OBS_ACCURACY));
                                }
                                if (interestAttr.contains(OBS_FRESHNESS)) {
                                    knowledgeTemp.setQoOAttribute(OBS_FRESHNESS, rawDataTemp.getQoOAttribute(OBS_FRESHNESS));
                                }
                            }

                            knowledgeTemp.addTimestamp("iQAS_out", System.currentTimeMillis());
                            return new ProducerRecord<byte[], String>(getTopicToPublish(), knowledgeTemp.toString());
                        })
                );


                builder.from(consumRecordToRawData.out())
                        .via(removeOutdatedObs)
                        .viaFanOut(bcast)
                        .toInlet(rawDataToProdRecord.in());

                builder.from(bcast)
                        .via(builder.add(new QoOReporter(getUniqueID(), getAssociatedRequest_id())))
                        .to(builder.add(Sink.foreach(elem -> getMonitorActor().tell(new QoOReportMsg((QoOReportMsg) elem), ActorRef.noSender()))));

                return new FlowShape<>(consumRecordToRawData.in(), rawDataToProdRecord.out());

            } else { // other observation levels are not supported
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

