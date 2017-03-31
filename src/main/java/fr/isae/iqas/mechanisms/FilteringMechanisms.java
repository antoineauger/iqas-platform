package fr.isae.iqas.mechanisms;

import akka.NotUsed;
import akka.japi.function.Predicate;
import akka.stream.javadsl.Flow;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by an.auger on 08/11/2016.
 */
public class FilteringMechanisms implements IFilteringMessages<ProducerRecord> {
    /**
     * @param incomingFlow
     * @param functionToApply
     * @return
     * @throws Exception
     */
    @Override
    public Flow<ProducerRecord, ProducerRecord, NotUsed> process(Flow<ProducerRecord, ProducerRecord, NotUsed> incomingFlow, Predicate<ProducerRecord> functionToApply) throws Exception {
        return incomingFlow.filter(functionToApply);
    }

    /**
     * @param incomingFlow
     * @param functionToApply
     * @return
     * @throws Exception
     */
    public Flow<ProducerRecord, ProducerRecord, NotUsed> filter(Flow<ProducerRecord, ProducerRecord, NotUsed> incomingFlow, Predicate<ProducerRecord> functionToApply) throws Exception {
        return process(incomingFlow, functionToApply);
    }

    /**
     * @param incomingFlow
     * @param functionToApply
     * @return
     * @throws Exception
     */
    public Flow<ProducerRecord, ProducerRecord, NotUsed> filterNot(Flow<ProducerRecord, ProducerRecord, NotUsed> incomingFlow, Predicate<ProducerRecord> functionToApply) throws Exception {
        return incomingFlow.filterNot(functionToApply);
    }
}
