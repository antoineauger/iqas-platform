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
import fr.isae.iqas.model.request.Request;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import scala.concurrent.Future;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static fr.isae.iqas.model.message.MAPEKInternalMsg.*;
import static fr.isae.iqas.model.request.State.Status.*;

/**
 * Created by an.auger on 13/09/2016.
 */

public class MonitorActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Map<String, Buffer> obsRateBuffer;
    private Map<String, Buffer> qooQualityBuffer;
    private Properties prop;
    private MongoController mongoController;
    private FusekiController fusekiController;

    public MonitorActor(Properties prop, MongoController mongoController, FusekiController fusekiController) {
        this.prop = prop;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;

        this.obsRateBuffer = new HashMap<>();
        this.qooQualityBuffer = new HashMap<>();
    }

    @Override
    public void preStart() {
        /*getContext().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                getSelf(), "tick", getContext().dispatcher(), null);*/
    }

    // override postRestart so we don't call preStart and schedule a new message
    @Override
    public void postRestart(Throwable reason) {
    }

    @Override
    public void onReceive(Object message) {
        /*if (message.equals("tick")) {
            // send another periodic tick after the specified delay
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(10, TimeUnit.SECONDS),
                    getSelf(), "tick", getContext().dispatcher(), null);

            System.out.println("It works!");
        } */

        /**
         * Debug messages
         */
        if (message instanceof String) {
            log.info("Received String message: {}", message);
        }
        /**
         * Request messages
         */
        else if (message instanceof Request) {
            Request requestTemp = (Request) message;
            log.info("Received Request : {}", requestTemp.getRequest_id());

            if (requestTemp.getCurrent_status() == SUBMITTED) { // Valid Request
                SymptomMsg symptomMsgToForward = new SymptomMsg(SymptomMAPEK.NEW, EntityMAPEK.REQUEST, requestTemp);
                forwardToAnalyzeActor(symptomMsgToForward);
            }
            else if (requestTemp.getCurrent_status() == REMOVED) { // Request deleted by the user
                SymptomMsg symptomMsgToForward = new SymptomMsg(SymptomMAPEK.REMOVED, EntityMAPEK.REQUEST, requestTemp);
                forwardToAnalyzeActor(symptomMsgToForward);
            }
            else if (requestTemp.getCurrent_status() == UPDATED) { // Existing Request updated by the user
                SymptomMsg symptomMsgToForward = new SymptomMsg(SymptomMAPEK.UPDATED, EntityMAPEK.REQUEST, requestTemp);
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
            if (tempObsRateReportMsg.getObsRateByTopic().size() > 0) {
                log.info("QoO report message: {} {}", tempObsRateReportMsg.getUniquePipelineID(), tempObsRateReportMsg.getObsRateByTopic().toString());
                if (!obsRateBuffer.containsKey(tempObsRateReportMsg.getUniquePipelineID())) {
                    obsRateBuffer.put(tempObsRateReportMsg.getUniquePipelineID(), new CircularFifoBuffer(5));
                }
                obsRateBuffer.get(tempObsRateReportMsg.getUniquePipelineID()).add(tempObsRateReportMsg);
            }

            // TODO to remove
            System.err.println("Obs Rate Buffer:");
            obsRateBuffer.forEach((k, v) -> {
                System.err.print(k + ": ");
                Iterator it = v.iterator();
                while (it.hasNext()) {
                    ObsRateReportMsg m = (ObsRateReportMsg) it.next();
                    System.err.println(m.getUniquePipelineID() + " | " + m.getObsRateByTopic().toString());
                }
            });
            System.err.println("------------------");
        }
        /**
         * QoOReportMsg messages
         */
        else if (message instanceof QoOReportMsg) {
            QoOReportMsg tempQoOReportMsg = (QoOReportMsg) message;
            if (tempQoOReportMsg.getQooAttributesMap().size() > 0) {
                log.info("QoO report message: {} {} {} {}", tempQoOReportMsg.getUniquePipelineID(), tempQoOReportMsg.getProducer(), tempQoOReportMsg.getRequestID(), tempQoOReportMsg.getQooAttributesMap().toString());
                if (!qooQualityBuffer.containsKey(tempQoOReportMsg.getRequestID())) {
                    qooQualityBuffer.put(tempQoOReportMsg.getRequestID(), new CircularFifoBuffer(5));
                }
                qooQualityBuffer.get(tempQoOReportMsg.getRequestID()).add(tempQoOReportMsg);
            }

            // TODO to remove
            System.err.println("Quality Buffer:");
            qooQualityBuffer.forEach((k, v) -> {
                System.err.print(k + ": ");
                Iterator it = v.iterator();
                while (it.hasNext()) {
                    QoOReportMsg m = (QoOReportMsg) it.next();
                    System.err.println(m.getUniquePipelineID() + " | " + m.getQooAttributesMap().toString() + " | " + m.getProducer() + " | " + m.getRequestID());
                }
            });
            System.err.println("------------------");
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
                + "/" + "analyzeActor").resolveOne(new Timeout(5, TimeUnit.SECONDS));
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

}

