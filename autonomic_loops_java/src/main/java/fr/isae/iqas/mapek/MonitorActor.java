package fr.isae.iqas.mapek;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import fr.isae.iqas.model.message.KafkaTopicMsg;
import fr.isae.iqas.model.message.QoOReportMsg;
import fr.isae.iqas.model.message.TerminatedMsg;

import java.util.Properties;

/**
 * Created by an.auger on 13/09/2016.
 */

public class MonitorActor extends UntypedActor {
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    public MonitorActor(Properties prop) {
    }

    @Override
    public void preStart() {
        /*getContext().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                getSelf(), "tick", getContext().dispatcher(), null);*/
    }

    // override postRestart so we don't call preStart and schedule a new message
    @Override
    public void postRestart(Throwable reason) {
    }

    @Override
    public void onReceive(Object message) {
        /*if (message.equals("tick")) {
            // send another periodic tick after the specified delay
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(10, TimeUnit.SECONDS),
                    getSelf(), "tick", getContext().dispatcher(), null);

            System.out.println("It works!");
        } */
        if (message instanceof KafkaTopicMsg) {
            log.info("Received KafkaTopicMsg message: {}", message);
            getSender().tell(message, getSelf());
        } else if (message instanceof String) {
            log.info("Received String message: {}", message);
        } else if (message instanceof QoOReportMsg) {
            QoOReportMsg tempQoOReportMsg = (QoOReportMsg) message;
            log.info("QoO report message: {} {} {}", tempQoOReportMsg.getPipeline_id(), tempQoOReportMsg.getProducer(), tempQoOReportMsg.getQooAttributesMap().toString());
        } else if (message instanceof TerminatedMsg) {
            TerminatedMsg terminatedMsg = (TerminatedMsg) message;
            if (terminatedMsg.getTargetToStop().path().equals(getSelf().path())) {
                log.info("Received TerminatedMsg message: {}", message);
                getContext().stop(self());
            }
        } else {
            unhandled(message);
        }
    }

    @Override
    public void postStop() {
    }

}

