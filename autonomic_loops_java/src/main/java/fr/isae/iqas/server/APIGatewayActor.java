package fr.isae.iqas.server;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Created by an.auger on 20/09/2016.
 */
public class APIGatewayActor extends UntypedActor {
    LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public APIGatewayActor() {

    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof String) {
            log.error("Received String message: {}", message);
            getSender().tell(message, getSelf());
        }
    }
}
