package fr.isae.iqas.utils;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedActorContext;
import akka.util.Timeout;
import scala.concurrent.Future;

import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 12/04/2017.
 */
public class ActorUtils {

    public static ActorSelection getAutonomicManagerActorFromDirectChildren(UntypedActorContext context, ActorRef self) {
        return context.actorSelection(self.path().parent());
    }

    public static ActorSelection getAnalyzeActorFromMAPEKchild(UntypedActorContext context, ActorRef self) {
        return context.actorSelection(self.path().parent().child("analyzeActor"));
    }

    public static ActorSelection getPlanActorFromMAPEKchild(UntypedActorContext context, ActorRef self) {
        return context.actorSelection(self.path().parent().child("planActor"));
        //return context.actorSelection(self.path().parent() + "/planActor").resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

    public static ActorSelection getPipelineWatcherActorFromMAPEKchild(UntypedActorContext context, ActorRef self) {
        return context.actorSelection(self.path().parent().parent().parent().child("pipelineWatcherActor"));
        //return context.actorSelection(self.path().parent().parent().parent() + "/pipelineWatcherActor").resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

    public static Future<ActorRef> getPipelineWatcherActor(UntypedActorContext context, String pathPipelineWatcherActor) {
        return context.actorSelection(pathPipelineWatcherActor).resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

}
