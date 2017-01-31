package fr.isae.iqas.mechanisms;

import akka.NotUsed;
import akka.japi.Function;
import akka.stream.javadsl.Flow;
import scala.concurrent.duration.FiniteDuration;

import java.util.List;

/**
 * Created by an.auger on 09/11/2016.
 */

@FunctionalInterface
public interface IAggregateMessages<K> {
    Flow<K, K, NotUsed> process(Flow<K, K, NotUsed> incomingFlow,
                                int nbKafkaMessages,
                                FiniteDuration windowDuration,
                                Function<List<K>, K> functionToApply) throws Exception;
}
