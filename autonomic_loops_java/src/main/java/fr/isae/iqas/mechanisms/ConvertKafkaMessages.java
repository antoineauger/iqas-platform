package fr.isae.iqas.mechanisms;

/**
 * Created by an.auger on 08/11/2016.
 */

@FunctionalInterface
public interface ConvertKafkaMessages<K, V> {
    V convert(K messageFromSource);
}
