package fr.isae.iqas.mechanisms;

import akka.NotUsed;
import akka.stream.javadsl.Flow;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.concurrent.duration.FiniteDuration;

import static fr.isae.iqas.mechanisms.Operators.*;

/**
 * Created by an.auger on 08/11/2016.
 */

public class AvailAdaptMechanisms {

    static FilteringMechanisms filteringTemplates = new FilteringMechanisms();
    static AggregateMechanisms aggregateTemplates = new AggregateMechanisms();
    static Flow<ConsumerRecord, ConsumerRecord, NotUsed> flowConsumerRecords = Flow.of(ConsumerRecord.class);
    static Flow<ProducerRecord, ProducerRecord, NotUsed> flowProducerRecords = Flow.of(ProducerRecord.class);

    /**
     * @param topicToPublish
     * @return
     */
    public static Flow<ConsumerRecord, ProducerRecord, NotUsed> f_convert_ConsumerToProducer(String topicToPublish) {
        IConvertRecords<ConsumerRecord, ProducerRecord> convertTools = r -> new ProducerRecord<byte[], String>(topicToPublish, String.valueOf(r.value()));
        return flowConsumerRecords.map(convertTools::convert);
    }

    /**
     * @param threshold
     * @return
     * @throws Exception
     */
    public static Flow<ProducerRecord, ProducerRecord, NotUsed> f_filter_ValuesGreaterThan(double threshold) throws Exception {
        return filteringTemplates.filter(flowProducerRecords, r -> Float.parseFloat((String) r.value()) > threshold);
    }

    /**
     * @param threshold
     * @return
     * @throws Exception
     */
    public static Flow<ProducerRecord, ProducerRecord, NotUsed> f_filter_ValuesLesserThan(double threshold) throws Exception {
        return filteringTemplates.filterNot(flowProducerRecords, r -> Float.parseFloat((String) r.value()) > threshold);
    }

    /**
     * @param nbRecords
     * @param topicToPublish
     * @return
     * @throws Exception
     */
    public static Flow<ProducerRecord, ProducerRecord, NotUsed> f_group_CountBasedMean(int nbRecords, String topicToPublish) throws Exception {
        return aggregateTemplates.groupCountBased(flowProducerRecords, nbRecords, recordList -> AVG(recordList, topicToPublish));
    }

    /**
     * @param t
     * @param topicToPublish
     * @return
     * @throws Exception
     */
    public static Flow<ProducerRecord, ProducerRecord, NotUsed> f_group_TimeBasedMean(FiniteDuration t, String topicToPublish) throws Exception {
        return aggregateTemplates.groupTimeBased(flowProducerRecords, t, recordList -> AVG(recordList, topicToPublish));
    }
}
