package fr.isae.iqas.mechanisms;

import akka.NotUsed;
import akka.japi.Function;
import akka.stream.javadsl.Flow;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.concurrent.duration.FiniteDuration;

import java.util.List;

/**
 * Created by an.auger on 09/11/2016.
 */
public class AggregateMechanisms implements IAggregateMessages<ProducerRecord> {
    /**
     * @param incomingFlow
     * @param nbKafkaMessages
     * @param windowDuration
     * @param functionToApply
     * @return
     * @throws Exception
     */
    @Override
    public Flow<ProducerRecord, ProducerRecord, NotUsed> process(Flow<ProducerRecord, ProducerRecord, NotUsed> incomingFlow,
                                                                 int nbKafkaMessages,
                                                                 FiniteDuration windowDuration,
                                                                 Function<List<ProducerRecord>, ProducerRecord> functionToApply) throws Exception {
        return incomingFlow.groupedWithin(nbKafkaMessages, windowDuration).map(recordList -> functionToApply.apply(recordList));
    }

    /**
     * @param incomingFlow
     * @param windowDuration
     * @param functionToApply
     * @return
     * @throws Exception
     */
    public Flow<ProducerRecord, ProducerRecord, NotUsed> groupTimeBased(Flow<ProducerRecord, ProducerRecord, NotUsed> incomingFlow,
                                                                        FiniteDuration windowDuration,
                                                                        Function<List<ProducerRecord>, ProducerRecord> functionToApply) throws Exception {
        return process(incomingFlow, Integer.MAX_VALUE, windowDuration, functionToApply);
    }

    /**
     * @param incomingFlow
     * @param nbKafkaMessages
     * @param functionToApply
     * @return
     * @throws Exception
     */
    public Flow<ProducerRecord, ProducerRecord, NotUsed> groupCountBased(Flow<ProducerRecord, ProducerRecord, NotUsed> incomingFlow,
                                                                         int nbKafkaMessages,
                                                                         Function<List<ProducerRecord>, ProducerRecord> functionToApply) throws Exception {
        return incomingFlow.grouped(nbKafkaMessages).map(recordList -> functionToApply.apply(recordList));
    }
}
