package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.model.jsonld.VirtualSensor;
import fr.isae.iqas.model.jsonld.VirtualSensorList;
import fr.isae.iqas.model.message.ObsRateReportMsg;
import fr.isae.iqas.model.message.QoOReportMsg;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.quality.ObservationRate;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.utils.ActorUtils;
import fr.isae.iqas.utils.HttpUtils;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static akka.dispatch.Futures.future;
import static fr.isae.iqas.model.message.MAPEKInternalMsg.*;
import static fr.isae.iqas.model.message.MAPEKInternalMsg.EntityMAPEK.*;
import static fr.isae.iqas.model.message.MAPEKInternalMsg.SymptomMAPEK.TOO_LOW;
import static fr.isae.iqas.model.request.State.Status.*;

/**
 * Created by an.auger on 13/09/2016.
 */

public class MonitorActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Properties prop;
    private MongoController mongoController;
    private FusekiController fusekiController;

    private FiniteDuration tickMAPEK = null;
    private int nbEventsBeforeSymptom = 5;
    private Map<String, Set<String>> mappingPipelinesRequests; // PipelineUniqueIDs <-> [concerned Request IDs]

    // For pinging Virtual Sensors
    private FiniteDuration pingSensorsInterval = null;
    private long lastPing = System.currentTimeMillis();

    // For ObsRate reporting
    private Map<String, Integer> numberObservedSymptomsObsRate; // RequestIDs <-> #Symptoms for ObsRate
    private Map<String, Long> startDateCount; // RequestIDs <-> Timestamps
    private Map<String, Integer> countByRequest; // RequestIDs <-> count (int)
    private Map<String, ObservationRate> minObsRateByRequest; // RequestIDs <-> Durations

    // For QoO reporting
    private Map<String, Buffer> qooQualityBuffer; // RequestIDs <-> [QoOReportMessages]

    /**
     * Monitor actor for the MAPE-K loop of the iQAS platform
     *
     * Emits symptoms for:
     *      -insufficient observation rate (parameter: obsRate_min) <-> OBS_RATE
     *      -expired observations (parameter: age_max) <-> OBS_FRESHNESS
     *      -inaccurate observations regarding sensor capabilities (inferred from the iQAS QoO-ontology) <-> OBS_ACCURACY
     *
     * @param prop
     * @param mongoController
     * @param fusekiController
     */
    public MonitorActor(Properties prop, MongoController mongoController, FusekiController fusekiController) {
        this.prop = prop;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;

        this.tickMAPEK = new FiniteDuration(Long.valueOf(prop.getProperty("tick_mapek_seconds")), TimeUnit.SECONDS);
        this.nbEventsBeforeSymptom = Integer.parseInt(prop.getProperty("nb_events_before_symptom"));
        this.mappingPipelinesRequests = new ConcurrentHashMap<>();

        this.pingSensorsInterval = new FiniteDuration(Long.parseLong(prop.getProperty("interval_to_ping_sensors_seconds")), TimeUnit.SECONDS);

        this.numberObservedSymptomsObsRate = new ConcurrentHashMap<>();
        this.startDateCount = new ConcurrentHashMap<>();
        this.countByRequest = new ConcurrentHashMap<>();
        this.minObsRateByRequest = new ConcurrentHashMap<>();

        this.qooQualityBuffer = new ConcurrentHashMap<>();
    }

    @Override
    public void preStart() {
        // TODO: uncomment
        //storeVirtualSensorStates();
        getContext().system().scheduler().scheduleOnce(
                tickMAPEK,
                getSelf(), "tick", getContext().dispatcher(), getSelf());
    }

    // override postRestart so we don't call preStart and schedule a new message
    @Override
    public void postRestart(Throwable reason) {
    }

    @Override
    public void onReceive(Object message) {
        // Tick messages
        if (message.equals("tick")) {
            // send another periodic tick after the specified delay
            getContext().system().scheduler().scheduleOnce(
                    tickMAPEK,
                    getSelf(), "tick", getContext().dispatcher(), getSelf());

            Map<String, List<String>> requestsWithInsuficientObsRate = new ConcurrentHashMap<>(); // UniquePipelineIDs <-> [ImpactedRequests]
            mappingPipelinesRequests.forEach((k, v) -> {
                for (String s : v) {
                    if (minObsRateByRequest.containsKey(s)) { // If there is a obsRate_min requirement for this Request
                        long step = new FiniteDuration(1, minObsRateByRequest.get(s).getUnit()).toMillis();
                        if (System.currentTimeMillis() - startDateCount.get(s) > step) {
                            if (countByRequest.get(s) < minObsRateByRequest.get(s).getValue()) {
                                numberObservedSymptomsObsRate.put(s, numberObservedSymptomsObsRate.get(s) + 1);
                                if (numberObservedSymptomsObsRate.get(s) >= nbEventsBeforeSymptom) {
                                    requestsWithInsuficientObsRate.putIfAbsent(k, new ArrayList<>());
                                    requestsWithInsuficientObsRate.get(k).add(s);
                                    numberObservedSymptomsObsRate.put(s, 0);
                                }
                            }
                            startDateCount.put(s, System.currentTimeMillis());
                            countByRequest.put(s, 0);
                        }
                    }
                }
            });

            for (Map.Entry<String, List<String>> entry : requestsWithInsuficientObsRate.entrySet()) {
                forwardToSpecifiedActor(new SymptomMsg(TOO_LOW, OBS_RATE, entry.getKey(), entry.getValue()), ActorUtils.getAnalyzeActor(getContext(), getSelf()));
            }

            if (System.currentTimeMillis() - lastPing > pingSensorsInterval.toMillis()) {
                lastPing = System.currentTimeMillis();
                // TODO: uncomment
                //storeVirtualSensorStates();
            }
        }
        // Request messages
        else if (message instanceof Request) {
            Request requestTemp = (Request) message;
            log.info("Received Request : {}", requestTemp.getRequest_id());

            if (requestTemp.getCurrent_status() == SUBMITTED) { // Valid Request
                SymptomMsg symptomMsgToForward = new SymptomMsg(SymptomMAPEK.NEW, EntityMAPEK.REQUEST, requestTemp);
                storeObsRateRequirements(requestTemp);
                qooQualityBuffer.put(requestTemp.getRequest_id(), new CircularFifoBuffer(nbEventsBeforeSymptom));
                numberObservedSymptomsObsRate.put(requestTemp.getRequest_id(), 0);
                startDateCount.put(requestTemp.getRequest_id(), System.currentTimeMillis());
                countByRequest.put(requestTemp.getRequest_id(), 0);

                // TODO
                log.error("MIN OBS_RATE REQ: " + minObsRateByRequest.toString());
                forwardToSpecifiedActor(symptomMsgToForward, ActorUtils.getAnalyzeActor(getContext(), getSelf()));
            }
            else if (requestTemp.getCurrent_status() == REMOVED) { // Request deleted by the user
                qooQualityBuffer.remove(requestTemp.getRequest_id());
                minObsRateByRequest.remove(requestTemp.getRequest_id());
                numberObservedSymptomsObsRate.remove(requestTemp.getRequest_id());
                startDateCount.remove(requestTemp.getRequest_id());
                countByRequest.remove(requestTemp.getRequest_id());

                SymptomMsg symptomMsgToForward = new SymptomMsg(SymptomMAPEK.REMOVED, EntityMAPEK.REQUEST, requestTemp);
                forwardToSpecifiedActor(symptomMsgToForward, ActorUtils.getAnalyzeActor(getContext(), getSelf()));
            }
            else if (requestTemp.getCurrent_status() == REJECTED) {
                // Do nothing since the Request has already been rejected
            }
            else { // Other cases should raise an error
                log.error("Unknown state for request " + requestTemp.getRequest_id() + " at this stage");
            }
        }
        // ObsRateReportMsg messages
        else if (message instanceof ObsRateReportMsg) {
            ObsRateReportMsg tempObsRateReportMsg = (ObsRateReportMsg) message;
            int totalObsFromSensors = 0;
            if (tempObsRateReportMsg.getObsRateByTopic().size() > 0) {
                log.info("QoO report message: {} {}", tempObsRateReportMsg.getUniquePipelineID(), tempObsRateReportMsg.getObsRateByTopic().toString());
                totalObsFromSensors = tempObsRateReportMsg.getObsRateByTopic().values().stream().mapToInt(Number::intValue).sum();
            }
            if (mappingPipelinesRequests.containsKey(tempObsRateReportMsg.getUniquePipelineID())) { // if there is a constraint on OBS_RATE for a Request using this Pipeline
                for (String s : mappingPipelinesRequests.get(tempObsRateReportMsg.getUniquePipelineID())) { // for all concerned Requests associated with this base Pipeline
                    if (countByRequest.containsKey(s)) {
                        countByRequest.put(s, countByRequest.get(s) + totalObsFromSensors);
                    }
                }
            }
        }
        // QoOReportMsg messages
        else if (message instanceof QoOReportMsg) {
            QoOReportMsg tempQoOReportMsg = (QoOReportMsg) message;
            if (tempQoOReportMsg.getQooAttributesMap().size() > 0) {
                log.info("QoO report message: {} {} {} {}", tempQoOReportMsg.getUniquePipelineID(), tempQoOReportMsg.getProducer(), tempQoOReportMsg.getRequestID(), tempQoOReportMsg.getQooAttributesMap().toString());
                qooQualityBuffer.putIfAbsent(tempQoOReportMsg.getRequestID(), new CircularFifoBuffer(5));
                qooQualityBuffer.get(tempQoOReportMsg.getRequestID()).add(new QoOReportMsg(tempQoOReportMsg));
            }
        }
        // SymptomMsg messages
        else if (message instanceof SymptomMsg) {
            SymptomMsg symptomMAPEKMsg = (SymptomMsg) message;
            if (symptomMAPEKMsg.getSymptom() == SymptomMAPEK.NEW && symptomMAPEKMsg.getAbout() == PIPELINE) { // Pipeline creation
                log.info("NEW IngestPipeline: {} {}", symptomMAPEKMsg.getUniqueIDPipeline(), symptomMAPEKMsg.getRequestID());
                mappingPipelinesRequests.putIfAbsent(symptomMAPEKMsg.getUniqueIDPipeline(), new HashSet<>());
                mappingPipelinesRequests.get(symptomMAPEKMsg.getUniqueIDPipeline()).add(symptomMAPEKMsg.getRequestID());
            }
            else if (symptomMAPEKMsg.getSymptom() == SymptomMAPEK.REMOVED && symptomMAPEKMsg.getAbout() == PIPELINE) { // Pipeline removal
                log.info("IngestPipeline " + symptomMAPEKMsg.getUniqueIDPipeline() + " is no longer active, removing it");
                numberObservedSymptomsObsRate.remove(symptomMAPEKMsg.getUniqueIDPipeline());
                mappingPipelinesRequests.remove(symptomMAPEKMsg.getUniqueIDPipeline());
            }
            else if (symptomMAPEKMsg.getSymptom() == SymptomMAPEK.UPDATED && symptomMAPEKMsg.getAbout() == SENSOR) { // Sensor UPDATE
                forwardToSpecifiedActor(new SymptomMsg(SymptomMAPEK.UPDATED, SENSOR), ActorUtils.getAnalyzeActor(getContext(), getSelf()));
            }
        }
        // TerminatedMsg messages
        else if (message instanceof TerminatedMsg) {
            TerminatedMsg terminatedMsg = (TerminatedMsg) message;
            if (terminatedMsg.getTargetToStop().path().equals(getSelf().path())) {
                log.info("Received TerminatedMsg message: {}", message);
                getContext().stop(self());
            }
        }
        else {
            unhandled(message);
        }
    }

    @Override
    public void postStop() {
    }

    private void forwardToSpecifiedActor(SymptomMsg symptomMsgToForward, Future<ActorRef> actorRefFuture) {
        actorRefFuture.onComplete(new OnComplete<ActorRef>() {
            @Override
            public void onComplete(Throwable t, ActorRef actor) throws Throwable {
                if (t != null) {
                    log.error("Unable to find the AnalyzeActor: " + t.toString());
                }
                else {
                    actor.tell(symptomMsgToForward, getSelf());
                }
            }
        }, getContext().dispatcher());
    }

    private void storeObsRateRequirements(Request incomingRequest) {
        if (incomingRequest.getQooConstraints().getIqas_params().containsKey("obsRate_min")
                || incomingRequest.getQooConstraints().getIqas_params().containsKey("obsRate_max")) { // if it expresses interest in OBS_RATE
            TimeUnit obsRateMaxUnit = null;
            long obsRateMinVal = -1;
            TimeUnit obsRateMinUnit = null;

            for (Object o : incomingRequest.getQooConstraints().getIqas_params().entrySet()) {
                Map.Entry pair = (Map.Entry) o;
                String k = (String) pair.getKey();
                String v = (String) pair.getValue();

                if (k.startsWith("obsRate_")) {
                    // We parse the obsRate according the pattern int/unit
                    String[] unitValueStr = v.split("/");

                    try {
                        long obsRateVal = Long.parseLong(unitValueStr[0]);
                        TimeUnit obsRateUnit;
                        if (unitValueStr.length == 2 && unitValueStr[1].equals("s")) {
                            obsRateUnit = TimeUnit.SECONDS;
                        }
                        else if (unitValueStr.length == 2 && unitValueStr[1].equals("min")) {
                            obsRateUnit = TimeUnit.MINUTES;
                        }
                        else if (unitValueStr.length == 2 && unitValueStr[1].equals("hour")) {
                            obsRateUnit = TimeUnit.HOURS;
                        }
                        else {
                            return;
                        }

                        // min or max obsRate requirement ?
                        String[] keyType = k.split("_");
                        if (keyType.length == 2 && keyType[1].equals("min")) {
                            obsRateMinVal = obsRateVal;
                            obsRateMinUnit = obsRateUnit;
                        }
                        else if (keyType.length == 2 && keyType[1].equals("max")) {
                            obsRateMaxUnit = obsRateUnit;
                        }
                    } catch (NumberFormatException | NullPointerException e) { // To avoid malformed QoO requirements
                        log.error("Error when trying to parse " + k + " parameter: " + e.toString());
                        obsRateMaxUnit = null;
                        obsRateMinUnit = null;
                        break;
                    }
                }
            }

            if (obsRateMinUnit != null && obsRateMaxUnit == null) { // Only obsRate_min is kept
                minObsRateByRequest.put(incomingRequest.getRequest_id(), new ObservationRate(obsRateMinVal, obsRateMinUnit));
            }
            else if (obsRateMinUnit == null && obsRateMaxUnit != null) { // Only obsRate_max is kept
                // Do nothing, obsRate_max is handled by the ThottlePipeline
            }
            else if (obsRateMinUnit != null && obsRateMaxUnit != null) { // Only obsRate_max is kept
                incomingRequest.getQooConstraints().getIqas_params().remove("obsRate_min");
            }
        }
    }

    private boolean storeVirtualSensorStates() {
        future(() -> fusekiController._findAllSensors(), context().dispatcher())
                .onComplete(new OnComplete<VirtualSensorList>() {
                    public void onComplete(Throwable throwable, VirtualSensorList virtualSensorList) {
                        if (throwable == null) { // Only continue if there was no error so far
                            Map<String, Boolean> connectedSensors = new ConcurrentHashMap<>(); // SensorIDs <-> connected (true/false)
                            for (VirtualSensor v : virtualSensorList.sensors) {
                                String sensor_id = v.sensor_id.split("#")[1];
                                boolean sensorIsConnected = HttpUtils.checkIfEndpointIsAvailable(v.endpoint.url);
                                connectedSensors.put(sensor_id, sensorIsConnected);
                            }
                            forwardToSpecifiedActor(new SymptomMsg(SymptomMAPEK.CONNECTION_REPORT, EntityMAPEK.SENSOR, connectedSensors),
                                    ActorUtils.getAutonomicManagerActorFromDirectChildren(getContext(), getSelf()));
                            forwardToSpecifiedActor(new SymptomMsg(SymptomMAPEK.CONNECTION_REPORT, EntityMAPEK.SENSOR, connectedSensors),
                                    ActorUtils.getAnalyzeActor(getContext(), getSelf()));
                        }
                    }
                }, context().dispatcher());

        return true;
    }

}

