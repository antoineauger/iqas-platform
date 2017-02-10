package fr.isae.iqas.server;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.mapek.AutoManagerActor;
import fr.isae.iqas.model.message.RESTRequestMsg;
import fr.isae.iqas.model.request.Request;
import fr.isae.iqas.model.request.State;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static fr.isae.iqas.model.message.RESTRequestMsg.RequestSubject.*;
import static fr.isae.iqas.model.request.State.Status.*;

/**
 * Created by an.auger on 20/09/2016.
 * The APIGatewayActor is in charge of pre-determine if incoming Requests can be satisfy or not
 * It is also responsible of storing and maintaining a repository with all Requests
 */
public class APIGatewayActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Map<String, ArrayList<Request>> registeredRequestsByApp;

    private Properties prop;
    private MongoController mongoController;
    private FusekiController fusekiController;
    private ActorRef autoManager;

    //TODO: to remove, only for testing with randomness
    private Random randomGenerator;

    public APIGatewayActor(Properties prop, MongoController mongoController, FusekiController fusekiController) {
        this.prop = prop;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;
        this.registeredRequestsByApp = new ConcurrentHashMap<>();

        this.autoManager = getContext()
                .actorOf(Props.create(AutoManagerActor.class, this.prop, this.mongoController, this.fusekiController), "autoManager");

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
        if (message instanceof RESTRequestMsg) {
            RESTRequestMsg.RequestSubject requestSubject = ((RESTRequestMsg) message).getRequestSubject();
            Request incomingRequest = ((RESTRequestMsg) message).getRequest();

            log.info("Received " + requestSubject + " action for request with id " + incomingRequest.getRequest_id());

            List<State.Status> statusesToRetrieve = new ArrayList<>();
            statusesToRetrieve.add(CREATED);
            statusesToRetrieve.add(ENFORCED);
            statusesToRetrieve.add(SUBMITTED);

            /*ArrayList<Request> registeredRequestsForApp = mongoController
                    .getFilteredRequestsByApp(incomingRequest.getApplication_id(), statusesToRetrieve).get();

            log.info("Application " + incomingRequest.getApplication_id() +
                    " has " + registeredRequestsForApp.size() + " registered requests.");*/

            if (requestSubject.equals(POST)) { // Creation
                CompletableFuture<Boolean> insertSuccess = mongoController.putRequest(incomingRequest);
                insertSuccess.whenComplete((result, throwable) -> {
                    if (result) {
                        autoManager.tell(incomingRequest, getSelf());
                    }
                    else {
                        log.error("Insert of the Request " + incomingRequest.getRequest_id() + " has failed. " +
                                "Not telling anything to Autonomic Managers...");
                    }
                });
            }
            else if (requestSubject.equals(PUT)) { // Update
                // TODO
            }
            else if (requestSubject.equals(GET)) {
                log.error("This should never happen: GET responsibility is directly handled by RESTServer");
            }
            else if (requestSubject.equals(DELETE)) { // Deletion
                CompletableFuture<Boolean> test = mongoController.deleteRequest(incomingRequest.getRequest_id());
                test.thenApply((result) -> {
                    if (result) {
                        log.info("Request with id " + incomingRequest.getRequest_id() + " successfully deleted!");
                        // TODO: warn auto manager to free resources
                    }
                    else {
                        log.warning("Unable to delete request with id " + incomingRequest.getRequest_id() + ". " +
                                "Operation skipped!");
                    }
                    return result;
                });
                // TODO: remove request from here
            }
            else {
                log.error("Unknown REST verb (" + requestSubject + ") for request with id " + incomingRequest.getRequest_id());
            }
        }

        /*if (message instanceof Request) { // Requests are received from RESTServer through the MongoRESTController
            log.info("Received Request: {}", message.toString());

            Request incomingRequest = (Request) message;

            List<State.Status> statusesToRetrieve = new ArrayList<>();
            statusesToRetrieve.add(State.Status.CREATED);
            statusesToRetrieve.add(State.Status.ENFORCED);
            statusesToRetrieve.add(State.Status.SUBMITTED);

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
                    // For now, all requests are forwarded to the AM
                    autoManager.tell(incomingRequest, getSelf());
                }
                else {
                    log.error("Insert of the Request " + incomingRequest.getRequest_id() + " has failed. " +
                            "Not telling anything to Autonomic Managers...");
                }
            }

            // We do not acknowledge the message since it was coming from REST server
        }*/
    }
}
