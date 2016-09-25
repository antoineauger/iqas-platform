package fr.isae.iqas.server;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.model.Request;

/**
 * Created by an.auger on 20/09/2016.
 */
public class APIGatewayActor extends UntypedActor {
    LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    MongoController mongoController;

    public APIGatewayActor(MongoController mongoController) {
        this.mongoController = mongoController;
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof Request) {

            Request receivedRequest = (Request) message;
            log.info("Received Request: {}", message.toString());

            // We do not acknowledge the message since it was coming from REST server
        }
        else if (message instanceof String) {
            log.error("Received String message: {}", message);
            //getSender().tell(message, getSelf());
        }
    }
}
