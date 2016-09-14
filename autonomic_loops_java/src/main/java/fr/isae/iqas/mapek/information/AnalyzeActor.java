package fr.isae.iqas.mapek.information;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.mapek.event.AddSensorSource;

/**
 * Created by an.auger on 13/09/2016.
 */
public class AnalyzeActor extends UntypedActor {
    LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final ActorRef monitor;

    public AnalyzeActor(ActorRef monitor) {
        this.monitor = monitor;
        this.monitor.tell(new AddSensorSource(), this.getSelf());
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof String) {
            log.info("Received String message: {}", message);
            getSender().tell(message, getSelf());
        } else
            unhandled(message);
    }
}
