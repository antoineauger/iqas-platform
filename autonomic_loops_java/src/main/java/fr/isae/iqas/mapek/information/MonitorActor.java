package fr.isae.iqas.mapek.information;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import fr.isae.iqas.mapek.event.AddSensorSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 13/09/2016.
 */

public class MonitorActor extends UntypedActor {
    LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final ActorRef kafkaActor;
    private final Materializer materializer;

    public MonitorActor(ActorRef kafkaActor, Materializer materializer) {
        this.kafkaActor = kafkaActor;
        this.materializer = materializer;
    }

    @Override
    public void preStart() {

        Consumer.plainExternalSource(kafkaActor, Subscriptions.assignment(new TopicPartition("topic1", 0)))
                .runWith(Sink.foreach(a -> System.out.println(a)), materializer);

        /*Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("topic1"))
                .runWith(Sink.foreach(a -> System.out.println(a)), materializer);*/

        getContext().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                getSelf(), "tick", getContext().dispatcher(), null);
    }

    // override postRestart so we don't call preStart and schedule a new message
    @Override
    public void postRestart(Throwable reason) {
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message.equals("tick")) {
            // send another periodic tick after the specified delay
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(10, TimeUnit.SECONDS),
                    getSelf(), "tick", getContext().dispatcher(), null);

            System.out.println("It works!");
        }
        else if (message instanceof AddSensorSource) {
            log.info("Received AddSensorSource message: {}", message);
            getSender().tell(message, getSelf());
        }
        else if (message instanceof String) {
            log.info("Received String message: {}", message);
            getSender().tell(message, getSelf());
        }
        else {
            unhandled(message);
        }
    }

}

