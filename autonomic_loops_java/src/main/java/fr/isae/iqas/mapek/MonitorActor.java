package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.dispatch.OnComplete;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.util.Timeout;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.model.message.QoOReportMsg;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.model.request.State;
import scala.concurrent.Future;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static fr.isae.iqas.model.message.MAPEKInternalMsg.*;

/**
 * Created by an.auger on 13/09/2016.
 */

public class MonitorActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Properties prop;
    private MongoController mongoController;
    private FusekiController fusekiController;

    public MonitorActor(Properties prop, MongoController mongoController, FusekiController fusekiController) {
        this.prop = prop;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;
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

            SymptomMsg symptomMsgToForward = null;
            if (requestTemp.getCurrent_status() == State.Status.SUBMITTED) { // Valid Request
                //TODO check if request is new or it is an update

                symptomMsgToForward = new SymptomMsg(SymptomMAPEK.NEW, EntityMAPEK.REQUEST, requestTemp);
            }
            else if (requestTemp.getCurrent_status() == State.Status.REMOVED) { // Request deleted by the user
                symptomMsgToForward = new SymptomMsg(SymptomMAPEK.REMOVED, EntityMAPEK.REQUEST, requestTemp);
            }
            else if (requestTemp.getCurrent_status() == State.Status.REJECTED) {
                // Do nothing since the Request has already been rejected
            }
            else { // Other cases should raise an error
                log.error("Unknown state for request " + requestTemp.getRequest_id() + " at this stage");
            }

            if (symptomMsgToForward != null) {
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
        /**
         * QoOReportMsg messages
         */
        else if (message instanceof QoOReportMsg) {
            QoOReportMsg tempQoOReportMsg = (QoOReportMsg) message;
            log.info("QoO report message: {} {} {} {}", tempQoOReportMsg.getPipeline_id(), tempQoOReportMsg.getProducer(), tempQoOReportMsg.getRequest_id(), tempQoOReportMsg.getQooAttributesMap().toString());
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

}

