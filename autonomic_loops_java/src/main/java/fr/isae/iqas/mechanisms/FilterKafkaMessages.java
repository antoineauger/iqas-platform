package fr.isae.iqas.mechanisms;

import akka.NotUsed;
import akka.japi.function.Predicate;
import akka.stream.javadsl.Flow;

/**
 * Created by an.auger on 08/11/2016.
 */

@FunctionalInterface
public interface FilterKafkaMessages<T> {
    Flow<T, T, NotUsed> process(Flow<T, T, NotUsed> incomingFlow, Predicate<T> functionToApply) throws Exception;
}