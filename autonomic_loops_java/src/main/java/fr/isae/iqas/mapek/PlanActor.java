package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import fr.isae.iqas.model.message.MAPEKInternalMsg;
import fr.isae.iqas.model.message.TerminatedMsg;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 13/09/2016.
 */
public class PlanActor extends UntypedActor {
    private String actorNameToResolve = "executeActor";

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Properties prop = null;
    private Set<String> topicsToPullFrom = new HashSet<>();
    private String topicToPublish = null;

    private Map<String, ActorRef> execActorsRefs = new HashMap<>();

    public PlanActor(Properties prop) {
        this.prop = prop;
        this.topicsToPullFrom.add("topic1");
        this.topicToPublish = "topic2";
    }

    @Override
    public void preStart() {
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof TerminatedMsg) {
            TerminatedMsg terminatedMsg = (TerminatedMsg) message;
            ActorRef actorRefToStop = terminatedMsg.getTargetToStop();
            if (terminatedMsg.getTargetToStop().path().equals(getSelf().path())) {
                log.info("Received TerminatedMsg message: {}", message);
                getContext().stop(self());
            }
            else {
                gracefulStop(actorRefToStop);
                execActorsRefs.remove(actorNameToResolve);
            }
        } else if (message instanceof MAPEKInternalMsg.RFCMsg) {
            log.info("Received RFCMsg message: {}", message);

            //TODO for now the remedyToPlan is hardcoded!
            MAPEKInternalMsg.RFCMsg receivedRFCMsg = (MAPEKInternalMsg.RFCMsg) message;
            String remedyToPlan = "SimpleFilteringPipeline";

            if (execActorsRefs.containsKey(actorNameToResolve)) { // if reference found, the corresponding actor has been started
                ActorRef actorRefToStop = execActorsRefs.get(actorNameToResolve);
                gracefulStop(actorRefToStop);

                ActorRef actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class, prop, topicsToPullFrom, topicToPublish, remedyToPlan));
                execActorsRefs.put(actorNameToResolve, actorRefToStart);
                log.info("Successfully started " + actorRefToStart.path());
            } else {
                ActorRef actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class, prop, topicsToPullFrom, topicToPublish, remedyToPlan));
                execActorsRefs.put(actorNameToResolve, actorRefToStart);
                log.info("Successfully started " + actorRefToStart.path());
            }
        }
    }

    private void gracefulStop(ActorRef actorRefToStop) {
        log.info("Trying to stop " + actorRefToStop.path().name());
        try {
            Future<Boolean> stopped = Patterns.gracefulStop(actorRefToStop, Duration.create(5, TimeUnit.SECONDS), new TerminatedMsg(actorRefToStop));
            Await.result(stopped, Duration.create(5, TimeUnit.SECONDS));
            log.info("Successfully stopped " + actorRefToStop.path());
        } catch (Exception e) {
            log.error(e.toString());
        }
    }
}
