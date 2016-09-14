package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import fr.isae.iqas.mapek.event.AddKafkaTopic;
import fr.isae.iqas.mapek.information.AnalyzeActor;
import fr.isae.iqas.mapek.information.MonitorActor;

/**
 * Created by an.auger on 13/09/2016.
 */
public class Test extends UntypedActor {

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("MySystem");

        // Actors for Information Layer
        final ActorRef infoMonitorActor = system.actorOf(Props.create(MonitorActor.class), "infoMonitorActor");
        final ActorRef infoAnalyzeActor = system.actorOf(Props.create(AnalyzeActor.class, infoMonitorActor), "infoAnalyzeActor");


        infoMonitorActor.tell(new AddKafkaTopic("topic1"), infoAnalyzeActor);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        infoMonitorActor.tell(new AddKafkaTopic("topic2"), infoAnalyzeActor);


        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        infoMonitorActor.tell(new AddKafkaTopic("terminate"), infoAnalyzeActor);
    }

    @Override
    public void onReceive(Object o) throws Throwable {

    }
}
