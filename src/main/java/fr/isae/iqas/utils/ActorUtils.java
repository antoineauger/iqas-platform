package fr.isae.iqas.utils;

import akka.actor.ActorRef;
import akka.actor.UntypedActorContext;
import akka.util.Timeout;
import scala.concurrent.Future;

import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 12/04/2017.
 */
public class ActorUtils {

    public static Future<ActorRef> getAutonomicManagerActorFromDirectChildren(UntypedActorContext context, ActorRef self) {
        return context.actorSelection(self.path().parent()).resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

    public static Future<ActorRef> getAnalyzeActor(UntypedActorContext context, ActorRef self) {
        return context.actorSelection(self.path().parent() + "/analyzeActor").resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

    public static Future<ActorRef> getPlanActor(UntypedActorContext context, ActorRef self) {
        return context.actorSelection(self.path().parent() + "/planActor").resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

    public static Future<ActorRef> getPipelineWatcherActor(UntypedActorContext context, ActorRef self) {
        return context.actorSelection(self.path().parent().parent().parent() + "/pipelineWatcherActor").resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

    public static Future<ActorRef> getPipelineWatcherActor(UntypedActorContext context, String pathPipelineWatcherActor) {
        return context.actorSelection(pathPipelineWatcherActor).resolveOne(new Timeout(5, TimeUnit.SECONDS));
    }

}
