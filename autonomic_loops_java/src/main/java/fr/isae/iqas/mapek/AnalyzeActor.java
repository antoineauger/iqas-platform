package fr.isae.iqas.mapek;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.model.messages.TerminatedMsg;

/**
 * Created by an.auger on 13/09/2016.
 */
public class AnalyzeActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public AnalyzeActor() {
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof TerminatedMsg) {
            log.info("Received TerminatedMsg message: {}", message);
            getSender().tell(message, getSelf());
            getContext().stop(self());
        }
    }
}
