package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.database.MongoController;
import fr.isae.iqas.model.messages.RFCMsg;
import fr.isae.iqas.model.messages.TerminatedMsg;
import fr.isae.iqas.model.request.Request;

import java.util.Properties;

/**
 * Created by an.auger on 25/09/2016.
 */

//TODO Create generic types for actor creation (ManagerActor, MonitorActor, AnalyzeActor, PlanActor, ExecuteActor)
public class ManagerActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private MongoController mongoController;

    private boolean processingActivated;

    private ActorRef monitorActor = null;
    private ActorRef analyzeActor = null;
    private ActorRef planActor = null;
    private ActorRef executeActor = null;

    public ManagerActor(Properties prop, MongoController mongoController) {
        this.mongoController = mongoController;

        //monitorActor = getContext().actorOf(Props.create(MonitorActor.class, prop), "monitorInfo");
        //analyzeActor = getContext().actorOf(Props.create(AnalyzeActor.class, prop), "analyzeInfo");
        planActor = getContext().actorOf(Props.create(PlanActor.class, prop), "planInfo");
        //executeActor = getContext().actorOf(Props.create(ExecuteActor.class, prop), "executeInfo");

        processingActivated = false;
    }

    @Override
    public void preStart() {

    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof TerminatedMsg) {
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
        else if (message instanceof Request) {
            if (processingActivated) {
                planActor.tell(new RFCMsg("none"), getSelf());
                processingActivated = false;
            }
            else {
                planActor.tell(new RFCMsg("testGraph"), getSelf());
                processingActivated = true;
            }
        }
    }
}
