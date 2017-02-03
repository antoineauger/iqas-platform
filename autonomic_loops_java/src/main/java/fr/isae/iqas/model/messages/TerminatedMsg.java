package fr.isae.iqas.model.messages;

import akka.actor.ActorRef;

/**
 * Created by an.auger on 08/11/2016.
 */
public class TerminatedMsg {
    ActorRef targetToStop;

    public TerminatedMsg(ActorRef targetToStop) {
        this.targetToStop = targetToStop;
    }

    public ActorRef getTargetToStop() {
        return targetToStop;
    }
}
