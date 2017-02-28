package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.model.message.MAPEKInternalMsg;
import fr.isae.iqas.model.message.TerminatedMsg;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 13/09/2016.
 */
public class PlanActor extends UntypedActor {
    private String actorNameToResolve = "executeActor";

    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Properties prop;
    private MongoController mongoController;
    private FusekiController fusekiController;
    private ActorRef kafkaAdminActor;

    private Map<String, ActorRef> execActorsRefs = new HashMap<>();

    public PlanActor(Properties prop, MongoController mongoController, FusekiController fusekiController, ActorRef kafkaAdminActor) {
        this.prop = prop;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;
        this.kafkaAdminActor = kafkaAdminActor;
    }

    @Override
    public void preStart() {
    }

    @Override
    public void onReceive(Object message) throws Exception {
        /**
         * ActionMsg messages
         */
        if (message instanceof MAPEKInternalMsg.ActionMsg) {
            log.info("Received RFCMsg message: {}", message);

            MAPEKInternalMsg.ActionMsg receivedActionMsg = (MAPEKInternalMsg.ActionMsg) message;

            if (execActorsRefs.containsKey(actorNameToResolve)) { // if reference found, the corresponding actor has been started
                ActorRef actorRefToStop = execActorsRefs.get(actorNameToResolve);
                gracefulStop(actorRefToStop);

                ActorRef actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class,
                        prop,
                        receivedActionMsg.getAttachedObject1(),
                        receivedActionMsg.getAttachedObject2(),
                        receivedActionMsg.getAttachedObject3(),
                        receivedActionMsg.getAssociatedRequest_id()));
                execActorsRefs.put(actorNameToResolve, actorRefToStart);
                log.info("Successfully started " + actorRefToStart.path());
            } else {
                ActorRef actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class,
                        prop,
                        receivedActionMsg.getAttachedObject1(),
                        receivedActionMsg.getAttachedObject2(),
                        receivedActionMsg.getAttachedObject3(),
                        receivedActionMsg.getAssociatedRequest_id()));
                execActorsRefs.put(actorNameToResolve, actorRefToStart);
                log.info("Successfully started " + actorRefToStart.path());
            }
        }
        /**
         * TerminatedMsg messages
         */
        else if (message instanceof TerminatedMsg) {
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
