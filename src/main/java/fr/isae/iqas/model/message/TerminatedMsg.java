package fr.isae.iqas.model.message;

import akka.actor.ActorRef;

/**
 * Created by an.auger on 08/11/2016.
 */
public class TerminatedMsg {
    private ActorRef targetToStop;

    public TerminatedMsg(ActorRef targetToStop) {
        this.targetToStop = targetToStop;
    }

    public ActorRef getTargetToStop() {
        return targetToStop;
    }
}
