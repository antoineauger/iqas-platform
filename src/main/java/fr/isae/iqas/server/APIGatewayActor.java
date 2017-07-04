package fr.isae.iqas.server;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.Materializer;
import fr.isae.iqas.config.Config;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.kafka.KafkaAdminActor;
import fr.isae.iqas.mapek.AutonomicManagerActor;
import fr.isae.iqas.model.message.RESTRequestMsg;
import fr.isae.iqas.model.message.SymptomMsg;
import fr.isae.iqas.model.request.Request;

import java.util.Properties;

import static fr.isae.iqas.model.message.MAPEKenums.EntityMAPEK;
import static fr.isae.iqas.model.message.MAPEKenums.SymptomMAPEK;
import static fr.isae.iqas.model.message.RESTRequestMsg.RequestSubject.*;
import static fr.isae.iqas.model.request.State.Status.*;

/**
 * Created by an.auger on 20/09/2016.
 * The APIGatewayActor is in charge of pre-determine if incoming Requests can be satisfy or not
 * It is also responsible of storing and maintaining a repository with all Requests
 */
public class APIGatewayActor extends AbstractActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Properties prop;
    private MongoController mongoController;
    private FusekiController fusekiController;

    private ActorRef autoManager;
    private ActorRef kafkaAdminActor;

    public APIGatewayActor(Config iqasConfig, ActorSystem system, Materializer materializer, MongoController mongoController, FusekiController fusekiController) {
        this.prop = iqasConfig.getProp();
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;

        this.kafkaAdminActor = getContext().actorOf(Props.create(KafkaAdminActor.class, prop), "KafkaAdminActor");
        this.autoManager = getContext()
                .actorOf(Props.create(AutonomicManagerActor.class, iqasConfig, system, materializer, this.kafkaAdminActor, this.mongoController, this.fusekiController), "autoManager");
    }

    @Override
    public void preStart() {
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RESTRequestMsg.class, this::actionsOnRESTRequestMsg)
                .match(SymptomMsg.class, this::actionsOnSymptomMsgMsg)
                .build();
    }

    private void actionsOnRESTRequestMsg(RESTRequestMsg msg) {
        RESTRequestMsg.RequestSubject requestSubject = msg.getRequestSubject();
        Request incomingRequest = msg.getRequest();

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
        else if (requestSubject.equals(GET)) {
            log.error("This should never happen: GET responsibility is directly handled by RESTServer");
        }
        else if (requestSubject.equals(DELETE)) { // Deletion
            mongoController.getSpecificRequest(incomingRequest.getRequest_id()).whenComplete((retrievedRequest, throwable) -> {
                if (throwable == null) {
                    if (retrievedRequest.isInState(ENFORCED) || retrievedRequest.isInState(HEALED)) {
                        retrievedRequest.addLog("Request deleted by the user.");
                        retrievedRequest.updateState(REMOVED);
                        mongoController.updateRequest(retrievedRequest.getRequest_id(), retrievedRequest).whenComplete((result2, throwable2) -> {
                            if (result2) {
                                log.info("Request with id " + retrievedRequest.getRequest_id() + " successfully marked for deletion.");
                                autoManager.tell(retrievedRequest, getSelf());
                            } else {
                                log.warning("Unable to mark request " + retrievedRequest.getRequest_id() + " for deletion. " +
                                        "Operation skipped!");
                            }
                        });
                    }
                }
                else {
                    log.warning("Unable to retrieve request " + incomingRequest.getRequest_id() + ". Operation skipped!");
                }
            });
        }
        else {
            log.error("Unknown REST verb (" + requestSubject + ") for request with id " + incomingRequest.getRequest_id());
        }
    }

    private void actionsOnSymptomMsgMsg(SymptomMsg msg) {
        if (msg.getSymptom() == SymptomMAPEK.UPDATED && msg.getAbout() == EntityMAPEK.SENSOR) {
            autoManager.tell(msg, getSelf());
        }
    }
}
