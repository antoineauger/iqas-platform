package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.util.Timeout;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.model.message.ObsRateReportMsg;
import fr.isae.iqas.model.message.QoOReportMsg;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.quality.ObservationRate;
import fr.isae.iqas.model.quality.QoOAttribute;
import fr.isae.iqas.model.request.Request;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static fr.isae.iqas.model.message.MAPEKInternalMsg.*;
import static fr.isae.iqas.model.message.MAPEKInternalMsg.EntityMAPEK.OBS_RATE;
import static fr.isae.iqas.model.message.MAPEKInternalMsg.EntityMAPEK.PIPELINE;
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

    private Map<String, Integer> obsRateBuffer; // Request ID <-> #Symptoms
    private Map<String, Buffer> qooQualityBuffer; // Request ID <-> [QoOReportMessages]
    private Map<String, Long> startDateCount; // Request ID <-> Timestamps
    private Map<String, Integer> countByRequest; // Request ID <-> count (int)
    private Map<String, Set<String>> mappingPipelinesRequests; // PipelineUniqueID <-> [concerned Request IDs]
    private Map<String, ObservationRate> minObsRateByRequest; // Request ID <-> Durations
    private Map<String, ObservationRate> maxObsRateByRequest; // Request ID <-> Durations

    public MonitorActor(Properties prop, MongoController mongoController, FusekiController fusekiController) {
        this.prop = prop;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;

        this.qooQualityBuffer = new ConcurrentHashMap<>();
        this.startDateCount = new ConcurrentHashMap<>();
        this.countByRequest = new ConcurrentHashMap<>();
        this.obsRateBuffer = new ConcurrentHashMap<>();
        this.mappingPipelinesRequests = new ConcurrentHashMap<>();
        this.minObsRateByRequest = new ConcurrentHashMap<>();
        this.maxObsRateByRequest = new ConcurrentHashMap<>();
    }

    @Override
    public void preStart() {
        getContext().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                getSelf(), "tick", getContext().dispatcher(), getSelf());
    }

    // override postRestart so we don't call preStart and schedule a new message
    @Override
    public void postRestart(Throwable reason) {
    }

    @Override
    public void onReceive(Object message) {
        if (message.equals("tick")) {
            // send another periodic tick after the specified delay
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(1, TimeUnit.SECONDS),
                    getSelf(), "tick", getContext().dispatcher(), getSelf());

            Map<String, List<String>> requestsWithInsuficientObsRate = new ConcurrentHashMap<>(); // UniquePipelineIDs <-> [ImpactedRequests]
            mappingPipelinesRequests.forEach((k, v) -> {
                for (String s : v) {
                    long step = new FiniteDuration(1, minObsRateByRequest.get(s).getUnit()).toMillis();
                    if (System.currentTimeMillis() - startDateCount.get(s) > step) {
                        if (countByRequest.get(s) < minObsRateByRequest.get(s).getValue()) {
                            obsRateBuffer.put(s, obsRateBuffer.get(s) + 1);
                            if (obsRateBuffer.get(s) == 5) {
                                requestsWithInsuficientObsRate.putIfAbsent(k, new ArrayList<>());
                                requestsWithInsuficientObsRate.get(k).add(s);
                                obsRateBuffer.put(s, 0);
                            }
                        }
                        startDateCount.put(s, System.currentTimeMillis());
                        countByRequest.put(s, 0);
                    }
                }
            });

            for (Map.Entry<String, List<String>> entry : requestsWithInsuficientObsRate.entrySet()) {
                forwardToAnalyzeActor(new SymptomMsg(TOO_LOW, OBS_RATE, entry.getKey(), entry.getValue()));
            }
        }
        /**
         * Request messages
         */
        else if (message instanceof Request) {
            Request requestTemp = (Request) message;
            log.info("Received Request : {}", requestTemp.getRequest_id());

            if (requestTemp.getCurrent_status() == SUBMITTED) { // Valid Request
                SymptomMsg symptomMsgToForward = new SymptomMsg(SymptomMAPEK.NEW, EntityMAPEK.REQUEST, requestTemp);

                storeObsRateRequirements(requestTemp);
                qooQualityBuffer.put(requestTemp.getRequest_id(), new CircularFifoBuffer(5));
                obsRateBuffer.put(requestTemp.getRequest_id(), 0);
                startDateCount.put(requestTemp.getRequest_id(), System.currentTimeMillis());
                countByRequest.put(requestTemp.getRequest_id(), 0);

                log.error("MIN OBS_RATE REQ: " + minObsRateByRequest.toString());
                log.error("MAX OBS_RATE REQ: " + maxObsRateByRequest.toString());
                forwardToAnalyzeActor(symptomMsgToForward);
            }
            else if (requestTemp.getCurrent_status() == REMOVED) { // Request deleted by the user
                qooQualityBuffer.remove(requestTemp.getRequest_id());
                minObsRateByRequest.remove(requestTemp.getRequest_id());
                maxObsRateByRequest.remove(requestTemp.getRequest_id());
                obsRateBuffer.remove(requestTemp.getRequest_id());
                startDateCount.remove(requestTemp.getRequest_id());
                countByRequest.remove(requestTemp.getRequest_id());

                SymptomMsg symptomMsgToForward = new SymptomMsg(SymptomMAPEK.REMOVED, EntityMAPEK.REQUEST, requestTemp);
                forwardToAnalyzeActor(symptomMsgToForward);
            }
            else if (requestTemp.getCurrent_status() == UPDATED) { // Existing Request updated by the user
                SymptomMsg symptomMsgToForward = new SymptomMsg(SymptomMAPEK.UPDATED, EntityMAPEK.REQUEST, requestTemp);
                storeObsRateRequirements(requestTemp);
                forwardToAnalyzeActor(symptomMsgToForward);
            }
            else if (requestTemp.getCurrent_status() == REJECTED) {
                // Do nothing since the Request has already been rejected
            }
            else { // Other cases should raise an error
                log.error("Unknown state for request " + requestTemp.getRequest_id() + " at this stage");
            }
        }
        /**
         * ObsRateReportMsg messages
         */
        else if (message instanceof ObsRateReportMsg) {
            ObsRateReportMsg tempObsRateReportMsg = (ObsRateReportMsg) message;
            int totalObsFromSensors = 0;
            if (tempObsRateReportMsg.getObsRateByTopic().size() > 0) {
                log.info("QoO report message: {} {}", tempObsRateReportMsg.getUniquePipelineID(), tempObsRateReportMsg.getObsRateByTopic().toString());
                totalObsFromSensors = tempObsRateReportMsg.getObsRateByTopic().values().stream().mapToInt(Number::intValue).sum();
            }
            if (mappingPipelinesRequests.containsKey(tempObsRateReportMsg.getUniquePipelineID())) { // if there is a constraint on OBS_RATE for a Request using this Pipeline
                for (String s : mappingPipelinesRequests.get(tempObsRateReportMsg.getUniquePipelineID())) { // for all concerned Requests associated with this base Pipeline
                    countByRequest.put(s, countByRequest.get(s) + totalObsFromSensors);
                }
            }
        }
        /**
         * QoOReportMsg messages
         */
        else if (message instanceof QoOReportMsg) {
            /*QoOReportMsg tempQoOReportMsg = (QoOReportMsg) message;
            if (tempQoOReportMsg.getQooAttributesMap().size() > 0) {
                log.info("QoO report message: {} {} {} {}", tempQoOReportMsg.getUniquePipelineID(), tempQoOReportMsg.getProducer(), tempQoOReportMsg.getRequestID(), tempQoOReportMsg.getQooAttributesMap().toString());
                if (!qooQualityBuffer.containsKey(tempQoOReportMsg.getRequestID())) {
                    qooQualityBuffer.put(tempQoOReportMsg.getRequestID(), new CircularFifoBuffer(5));
                }
                qooQualityBuffer.get(tempQoOReportMsg.getRequestID()).add(new QoOReportMsg(tempQoOReportMsg));
            }*/

            // TODO to remove
            /*log.info("Quality Buffer:");
            qooQualityBuffer.forEach((k, v) -> {
                //log.info(k + ": ");
                Iterator it = v.iterator();
                while (it.hasNext()) {
                    QoOReportMsg m = (QoOReportMsg) it.next();
                    log.info(m.getUniquePipelineID() + " | " + m.getQooAttributesMap().toString() + " | " + m.getProducer() + " | " + m.getRequestID());
                }
            });
            log.info("------------------");*/
        }
        /**
         * SymptomMsg messages
         */
        else if (message instanceof SymptomMsg) {
            SymptomMsg symptomMAPEKMsg = (SymptomMsg) message;
            if (symptomMAPEKMsg.getSymptom() == SymptomMAPEK.NEW && symptomMAPEKMsg.getAbout() == PIPELINE) { // Pipeline creation
                log.info("New ObsRatePipeline: {} {}", symptomMAPEKMsg.getUniqueIDPipeline(), symptomMAPEKMsg.getRequestID());
                if (!mappingPipelinesRequests.containsKey(symptomMAPEKMsg.getUniqueIDPipeline())) {
                    mappingPipelinesRequests.put(symptomMAPEKMsg.getUniqueIDPipeline(), new HashSet<>());
                }
                mappingPipelinesRequests.get(symptomMAPEKMsg.getUniqueIDPipeline()).add(symptomMAPEKMsg.getRequestID());
            }
            else if (symptomMAPEKMsg.getSymptom() == SymptomMAPEK.REMOVED && symptomMAPEKMsg.getAbout() == PIPELINE) { // Pipeline removal
                log.info("ObsRatePipeline " + symptomMAPEKMsg.getUniqueIDPipeline() + " is no longer active, removing it");
                obsRateBuffer.remove(symptomMAPEKMsg.getUniqueIDPipeline());
                mappingPipelinesRequests.remove(symptomMAPEKMsg.getUniqueIDPipeline());
            }
        }
        /**
         * TerminatedMsg messages
         */
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

    private Future<ActorRef> getAnalyzeActor() {
        return getContext().actorSelection(getSelf().path().parent()
                + "/analyzeActor").resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

    private void forwardToAnalyzeActor(SymptomMsg symptomMsgToForward) {
        SymptomMsg finalSymptomMsgToForward = symptomMsgToForward;
        getAnalyzeActor().onComplete(new OnComplete<ActorRef>() {
            @Override
            public void onComplete(Throwable t, ActorRef analyzeActor) throws Throwable {
                if (t != null) {
                    log.error("Unable to find the AnalyzeActor: " + t.toString());
                }
                else {
                    analyzeActor.tell(finalSymptomMsgToForward, getSelf());
                }
            }
        }, getContext().dispatcher());
    }

    private void storeObsRateRequirements(Request incomingRequest) {
        if (incomingRequest.isInterestedIn(QoOAttribute.OBS_RATE)) { // if it expresses interest in OBS_RATE
            long obsRateMaxVal = -1;
            TimeUnit obsRateMaxUnit = null;
            long obsRateMinVal = -1;
            TimeUnit obsRateMinUnit = null;

            for (Object o : incomingRequest.getQooConstraints().getAdditional_params().entrySet()) {
                Map.Entry pair = (Map.Entry) o;
                String k = (String) pair.getKey();
                String v = (String) pair.getValue();

                if (k.startsWith("obsRate_")) {
                    // We parse the obsRate according the pattern int/unit
                    String[] unitValue = v.split("/");
                    long obsRateVal = Long.parseLong(unitValue[0]);
                    TimeUnit obsRateUnit = null;
                    if (unitValue.length == 2 && unitValue[1].equals("s")) {
                        obsRateUnit = TimeUnit.SECONDS;
                    }
                    else if (unitValue.length == 2 && unitValue[1].equals("min")) {
                        obsRateUnit = TimeUnit.MINUTES;
                    }
                    else if (unitValue.length == 2 && unitValue[1].equals("hour")) {
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
                        obsRateMaxVal = obsRateVal;
                        obsRateMaxUnit = obsRateUnit;
                    }
                }
            }

            if (obsRateMinUnit != null && obsRateMaxUnit == null) {
                minObsRateByRequest.put(incomingRequest.getRequest_id(), new ObservationRate(obsRateMinVal, obsRateMinUnit));
            }
            else if (obsRateMinUnit == null && obsRateMaxUnit != null) {
                maxObsRateByRequest.put(incomingRequest.getRequest_id(), new ObservationRate(obsRateMaxVal, obsRateMaxUnit));
            }
            else if (obsRateMinUnit != null && obsRateMaxUnit != null) {
                maxObsRateByRequest.put(incomingRequest.getRequest_id(), new ObservationRate(obsRateMaxVal, obsRateMaxUnit));
            }
        }
    }

}

