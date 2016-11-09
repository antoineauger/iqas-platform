package fr.isae.iqas.mapek.information;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.model.messages.AddKafkaTopic;
import fr.isae.iqas.model.messages.Terminated;

import java.util.Properties;

/**
 * Created by an.auger on 25/09/2016.
 */

//TODO Create generic types for actor creation (ManagerActor, MonitorActor, AnalyzeActor, PlanActor, ExecuteActor)
public class ManagerActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Properties prop;
    private ActorRef monitorActor;
    private ActorRef analyzeActor;
    private ActorRef planActor;
    private ActorRef executeActor;

    public ManagerActor(Properties prop) {
        this.prop = prop;
        monitorActor = getContext().actorOf(Props.create(MonitorActor.class, prop), "monitorInfo");
        //monitorActor.tell(new AddKafkaTopic("topic3"), getSelf());
    }

    @Override
    public void preStart() {

    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Terminated) {
            log.info("Received Terminated message: {}", message);
            getContext().system().stop(self());
        }
    }
}
