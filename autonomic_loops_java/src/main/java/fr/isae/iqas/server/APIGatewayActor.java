package fr.isae.iqas.server;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.kafka.KafkaAdminActor;
import fr.isae.iqas.mapek.AutoManagerActor;
import fr.isae.iqas.model.message.RESTRequestMsg;
import fr.isae.iqas.model.request.Request;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static fr.isae.iqas.model.message.RESTRequestMsg.RequestSubject.*;
import static fr.isae.iqas.model.request.State.Status.REMOVED;

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
    private ActorRef kafkaAdminActor;

    public APIGatewayActor(Properties prop, MongoController mongoController, FusekiController fusekiController) {
        this.prop = prop;
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;
        this.registeredRequestsByApp = new ConcurrentHashMap<>();


        this.kafkaAdminActor = getContext().actorOf(Props.create(KafkaAdminActor.class, prop), "KafkaAdminActor");
        this.autoManager = getContext()
                .actorOf(Props.create(AutoManagerActor.class, this.prop, this.kafkaAdminActor, this.mongoController, this.fusekiController), "autoManager");
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

            if (requestSubject.equals(POST)) { // Creation
                mongoController.putRequest(incomingRequest).whenComplete((result, throwable) -> {
                    if (result) {
                        autoManager.tell(incomingRequest, getSelf());
                    }
                    else {
                        log.error("Insert of the Request " + incomingRequest.getRequest_id() + " has failed. " +
                                "Not telling anything to Autonomic Managers.");
                    }
                });
            }
            else if (requestSubject.equals(PUT)) { // Update
                // TODO update request logic
            }
            else if (requestSubject.equals(GET)) {
                log.error("This should never happen: GET responsibility is directly handled by RESTServer");
            }
            else if (requestSubject.equals(DELETE)) { // Deletion
                mongoController.getSpecificRequest(incomingRequest.getRequest_id()).whenComplete((result, throwable) -> {
                    if (throwable == null && result.size() == 1) {
                        Request retrievedRequest = result.get(0);
                        retrievedRequest.addLog("Request deleted by the user.");
                        retrievedRequest.updateState(REMOVED);
                        mongoController.updateRequest(retrievedRequest.getRequest_id(), retrievedRequest).whenComplete((result2, throwable2) -> {
                            if (result2) {
                                log.info("Request with id " + retrievedRequest.getRequest_id() + " successfully marked for deletion.");
                                autoManager.tell(retrievedRequest, getSelf());
                            }
                            else {
                                log.warning("Unable to mark request " + retrievedRequest.getRequest_id() + " for deletion. " +
                                        "Operation skipped!");
                            }
                        });
                    }
                    else {
                        log.warning("Unable to retrieve request " + incomingRequest.getRequest_id() + ". " +
                                "Operation skipped!");
                    }
                });
            }
            else {
                log.error("Unknown REST verb (" + requestSubject + ") for request with id " + incomingRequest.getRequest_id());
            }
        }
    }
}
