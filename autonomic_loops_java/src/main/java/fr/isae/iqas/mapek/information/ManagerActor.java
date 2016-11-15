package fr.isae.iqas.mapek.information;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.model.Request;
import fr.isae.iqas.model.messages.RFC;
import fr.isae.iqas.model.messages.Terminated;

import java.util.Properties;

/**
 * Created by an.auger on 25/09/2016.
 */

//TODO Create generic types for actor creation (ManagerActor, MonitorActor, AnalyzeActor, PlanActor, ExecuteActor)
public class ManagerActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private boolean processingActivated;

    private Properties prop;
    private ActorRef monitorActor = null;
    private ActorRef analyzeActor = null;
    private ActorRef planActor = null;
    private ActorRef executeActor = null;

    public ManagerActor(Properties prop) {
        this.prop = prop;
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
        if (message instanceof Terminated) {
            log.info("Received Terminated message: {}", message);

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
            if(processingActivated) {
                planActor.tell(new RFC("none"), getSelf());
                processingActivated = false;
            }
            else {
                planActor.tell(new RFC("testGraph"), getSelf());
                processingActivated = true;
            }
        }
    }
}
