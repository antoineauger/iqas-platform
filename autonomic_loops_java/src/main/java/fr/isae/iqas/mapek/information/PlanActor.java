package fr.isae.iqas.mapek.information;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.AskTimeoutException;
import akka.pattern.Patterns;
import fr.isae.iqas.model.messages.RFC;
import fr.isae.iqas.model.messages.Terminated;
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
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Properties prop = null;
    private String topicToPullFrom = null;
    private String topicToPushTo = null;

    private Map<String, ActorRef> execActorsRefs = new HashMap<>();

    public PlanActor(Properties prop) {
        this.prop = prop;
        this.topicToPullFrom = "topic1";
        this.topicToPushTo = "topic2";
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Terminated) {
            log.info("Received Terminated message: {}", message);
            getContext().stop(self());
        } else if (message instanceof RFC) {
            log.info("Received RFC message: {}", message);

            RFC receivedRFC = (RFC) message;
            String remedyToPlan = receivedRFC.getRemedyToPlan();

            String actorNameToResolve = "executeActor";

            if (execActorsRefs.containsKey(actorNameToResolve)) { // if reference found, the corresponding actor has been started
                ActorRef actorRefToStop = execActorsRefs.get(actorNameToResolve);
                log.info("Stopping " + actorRefToStop.path().name());
                try {
                    Future<Boolean> stopped = Patterns.gracefulStop(actorRefToStop, Duration.create(10, TimeUnit.SECONDS), new Terminated());
                    Await.result(stopped, Duration.create(10, TimeUnit.SECONDS));
                    log.info("Successfully stopped " + actorRefToStop.path());

                    ActorRef actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class, prop, topicToPullFrom, topicToPushTo, remedyToPlan));
                    execActorsRefs.put(actorNameToResolve, actorRefToStart);
                    log.info("Successfully started " + actorRefToStart.path());
                } catch (AskTimeoutException e) {
                    // the actor wasn't stopped within 10 seconds
                    log.error(e.toString());
                }
            } else {
                ActorRef actorRefToStart = getContext().actorOf(Props.create(ExecuteActor.class, prop, topicToPullFrom, topicToPushTo, remedyToPlan));
                execActorsRefs.put(actorNameToResolve, actorRefToStart);
                log.info("Successfully started " + actorRefToStart.path());
            }
        }
    }
}
