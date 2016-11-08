package fr.isae.iqas.server;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.mapek.information.ManagerActor;
import fr.isae.iqas.model.Request;

import java.util.Properties;

/**
 * Created by an.auger on 20/09/2016.
 */
public class APIGatewayActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Properties prop;
    private MongoController mongoController;
    private ActorRef autoManagerRawData;
    private ActorRef autoManagerInfo;

    public APIGatewayActor(Properties prop, MongoController mongoController) {
        this.prop = prop;
        this.mongoController = mongoController;

        autoManagerRawData = getContext().actorOf(Props.create(ManagerActor.class, prop), "autoManagerRawData");
        //autoManagerInfo = getContext().actorOf(Props.create(ManagerActor.class, prop), "autoManagerInfo");
    }

    @Override
    public void preStart() {
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
