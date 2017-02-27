package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.database.FusekiController;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.model.message.MAPEKInternalMsg;
import fr.isae.iqas.model.message.TerminatedMsg;
import fr.isae.iqas.model.request.Request;

import java.util.Properties;

/**
 * Created by an.auger on 25/09/2016.
 */

public class AutoManagerActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private MongoController mongoController;
    private FusekiController fusekiController;

    private boolean processingActivated;

    private ActorRef monitorActor = null;
    private ActorRef analyzeActor = null;
    private ActorRef planActor = null;
    private ActorRef executeActor = null;

    public AutoManagerActor(Properties prop, MongoController mongoController, FusekiController fusekiController) {
        this.mongoController = mongoController;
        this.fusekiController = fusekiController;

        monitorActor = getContext().actorOf(Props.create(MonitorActor.class, prop), "monitorActor");
        //analyzeActor = getContext().actorOf(Props.create(AnalyzeActor.class, prop), "analyzeActor");
        planActor = getContext().actorOf(Props.create(PlanActor.class, prop), "planActor");
        //executeActor = getContext().actorOf(Props.create(ExecuteActor.class, prop), "executeActor");

        processingActivated = false;
    }

    @Override
    public void preStart() {

    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof TerminatedMsg) {
            TerminatedMsg terminatedMsg = (TerminatedMsg) message;
            if (terminatedMsg.getTargetToStop().path().equals(getSelf().path())) {
                log.info("Received TerminatedMsg message: {}", message);
                if (monitorActor != null) {
                    getContext().stop(monitorActor);
                }
                if (analyzeActor != null) {
                    getContext().stop(analyzeActor);
                }
                if (planActor != null) {
                    getContext().stop(planActor);
                }
                if (executeActor != null) {
                    getContext().stop(executeActor);
                }
                getContext().stop(self());
            }
        }
        else if (message instanceof Request) {
            // TODO
            Request requestTemp = (Request) message;
            if (processingActivated) {
                planActor.tell(new MAPEKInternalMsg.RFCMsg("none", requestTemp.getRequest_id()), getSelf());
                processingActivated = false;
            }
            else {
                planActor.tell(new MAPEKInternalMsg.RFCMsg("testGraph", requestTemp.getRequest_id()), getSelf());
                processingActivated = true;
            }
        }
    }
}
