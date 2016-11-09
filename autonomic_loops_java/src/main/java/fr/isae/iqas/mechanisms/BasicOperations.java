package fr.isae.iqas.mechanisms;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Comparator;
import java.util.List;

/**
 * Created by an.auger on 09/11/2016.
 */
public class BasicOperations {

    /**
     * @param recordList
     * @param topicToPublish
     * @return
     */
    public static ProducerRecord computeMean(List<ProducerRecord> recordList, String topicToPublish) {
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
    public static ProducerRecord takeMAX(List<ProducerRecord> recordList, String topicToPublish) {
        recordList.sort(Comparator.comparingDouble(r -> Float.parseFloat((String) r.value())));
        return new ProducerRecord<byte[], String>(topicToPublish, String.valueOf(recordList.get(recordList.size()-1)));
    }

    /**
     * @param recordList
     * @param topicToPublish
     * @return
     */
    public static ProducerRecord takeMIN(List<ProducerRecord> recordList, String topicToPublish) {
        recordList.sort(Comparator.comparingDouble(r -> Float.parseFloat((String) r.value())));
        return new ProducerRecord<byte[], String>(topicToPublish, String.valueOf(recordList.get(0)));
    }

}
