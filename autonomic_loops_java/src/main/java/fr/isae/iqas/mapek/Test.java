package fr.isae.iqas.mapek;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.kafka.ConsumerSettings;
import akka.kafka.KafkaConsumerActor;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import fr.isae.iqas.mapek.information.MonitorActor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Created by an.auger on 13/09/2016.
 */
public class Test extends UntypedActor {

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("MySystem");
        final Materializer materializer = ActorMaterializer.create(system);

        final ConsumerSettings<byte[], String> consumerSettings =
                ConsumerSettings.create(system, new ByteArrayDeserializer(), new StringDeserializer())
                        .withBootstrapServers("localhost:9092")
                        .withGroupId("group1")
                        .withClientId("client1")
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Default KafkaActor for reuse
        final ActorRef kafkaActor = system.actorOf(KafkaConsumerActor.props(consumerSettings));

        // Actors for Information Layer
        final ActorRef infoMonitorActor = system.actorOf(Props.create(MonitorActor.class, kafkaActor, materializer), "infoMonitorActor");
        //final ActorRef infoAnalyzeActor = system.actorOf(Props.create(AnalyzeActor.class, infoMonitorActor), "infoAnalyzeActor");

    }

    @Override
    public void onReceive(Object o) throws Throwable {

    }
}
