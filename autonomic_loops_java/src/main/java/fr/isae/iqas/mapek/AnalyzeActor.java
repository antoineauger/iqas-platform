package fr.isae.iqas.mapek;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.model.messages.Terminated;

/**
 * Created by an.auger on 13/09/2016.
 */
public class AnalyzeActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public AnalyzeActor() {
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Terminated) {
            log.info("Received Terminated message: {}", message);
            getSender().tell(message, getSelf());
            getContext().stop(self());
        }
    }
}
