package fr.isae.iqas.server;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.mapek.ManagerActor;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.model.request.Status;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

/**
 * Created by an.auger on 20/09/2016.
 */
public class APIGatewayActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Properties prop;
    private MongoController mongoController;
    private ActorRef autoManagerRawData;
    private ActorRef autoManagerInfo;

    //TODO: to remove, only for testing with randomness
    private Random randomGenerator;

    public APIGatewayActor(Properties prop, MongoController mongoController) {
        this.prop = prop;
        this.mongoController = mongoController;

        //autoManagerRawData = getContext().actorOf(Props.create(ManagerActor.class, prop), "autoManagerRawData");
        autoManagerInfo = getContext()
                .actorOf(Props.create(ManagerActor.class, this.prop, this.mongoController), "autoManagerInfo");

        //TODO: to remove, only for testing with randomness
        /*
        randomGenerator  = new Random();
        int randomInt = randomGenerator.nextInt(2);
        if (randomInt == 0) {
            incomingRequest.updateState(Status.DONE);
        }*/
    }

    @Override
    public void preStart() {
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof Request) { // Requests are received from RESTServer through the MongoRESTController
            log.info("Received Request: {}", message.toString());

            Request incomingRequest = (Request) message;

            //TODO: Build request, Possible? YES / NO, Get ticket number, Forward request to API gateway
            ArrayList<Status> statusesToRetrieve = new ArrayList<>();
            statusesToRetrieve.add(Status.CREATED);
            statusesToRetrieve.add(Status.ENFORCED);
            statusesToRetrieve.add(Status.SUBMITTED);

            ArrayList<Request> registeredRequestsForApp = mongoController
                    .getFilteredRequestsByApp(incomingRequest.getApplication_id(), statusesToRetrieve).get();

            log.info("Application " + incomingRequest.getApplication_id() +
                    " has " + registeredRequestsForApp.size() + " registered requests.");

            if (registeredRequestsForApp.contains(incomingRequest)) {
                log.info("Request " + incomingRequest.getRequest_id() + " has already been inserted!");
            }
            else {
                log.info("Request " + incomingRequest.getRequest_id() + " has not been inserted yet.");

                boolean insertSuccess = mongoController.putRequest(incomingRequest).get();
                if (insertSuccess) {
                    // TODO: route request to Raw Data or Info layers
                    // For now, all requests are forwarded to the information AM
                    autoManagerInfo.tell(incomingRequest, getSelf());
                }
                else {
                    log.error("Insert of the Request " + incomingRequest.getRequest_id() + " has failed. " +
                            "Not telling anything to Autonomic Managers...");
                }
            }

            // We do not acknowledge the message since it was coming from REST server
        }
    }
}
