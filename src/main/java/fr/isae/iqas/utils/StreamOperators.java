package fr.isae.iqas.utils;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Comparator;
import java.util.List;

/**
 * Created by an.auger on 09/11/2016.
 */
// TODO refactor, to keep?
public class StreamOperators {

    /**
     * @param recordList
     * @param topicToPublish
     * @return
     */
    public static ProducerRecord AVG(List<ProducerRecord> recordList, String topicToPublish) {
        double avg = 0.0;
        for (ProducerRecord r : recordList) {
            avg += Float.parseFloat((String) r.value());
        }
        avg /= recordList.size();
        return new ProducerRecord<byte[], String>(topicToPublish, String.valueOf(avg));
    }

    /**
     * @param recordList
     * @param topicToPublish
     * @return
     */
    public static ProducerRecord MAX(List<ProducerRecord> recordList, String topicToPublish) {
        recordList.sort(Comparator.comparingDouble(r -> Float.parseFloat((String) r.value())));
        return new ProducerRecord<byte[], String>(topicToPublish, String.valueOf(recordList.get(recordList.size()-1)));
    }

    /**
     * @param recordList
     * @param topicToPublish
     * @return
     */
    public static ProducerRecord MIN(List<ProducerRecord> recordList, String topicToPublish) {
        recordList.sort(Comparator.comparingDouble(r -> Float.parseFloat((String) r.value())));
        return new ProducerRecord<byte[], String>(topicToPublish, String.valueOf(recordList.get(0)));
    }

}
