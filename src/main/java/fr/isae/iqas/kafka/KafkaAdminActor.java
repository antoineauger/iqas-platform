package fr.isae.iqas.kafka;

import akka.actor.UntypedActor;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.Properties;

/**
 * Created by an.auger on 09/02/2017.
 */
public class KafkaAdminActor extends UntypedActor {
    private static final int sessionTimeoutMs = 10 * 1000;
    private static final int connectionTimeoutMs = 8 * 1000;

    private String zookeeperConnect;

    public KafkaAdminActor(Properties prop) {
        this.zookeeperConnect = prop.getProperty("zookeeper_endpoint_address") + ":" + prop.getProperty("zookeeper_endpoint_port");
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof KafkaTopicMsg) {
            KafkaTopicMsg kafkaTopicMsg = (KafkaTopicMsg) message;
            boolean result = false;
            if (kafkaTopicMsg.getTopicAction() == KafkaTopicMsg.TopicAction.CREATE) {
                result = createTopic(kafkaTopicMsg.getTopic());
            }
            else if (kafkaTopicMsg.getTopicAction() == KafkaTopicMsg.TopicAction.DELETE) {
                result = deleteTopic(kafkaTopicMsg.getTopic());
            }
            else if (kafkaTopicMsg.getTopicAction() == KafkaTopicMsg.TopicAction.RESET) {
                result = resetTopic(kafkaTopicMsg.getTopic());
            }
            getSender().tell(result, getSelf());
        }
    }

    private boolean resetTopic(String kafkaTopicToReset) {
        boolean firstOp = deleteTopic(kafkaTopicToReset);
        boolean secondOp = createTopic(kafkaTopicToReset);

        return (firstOp && secondOp);
    }

    private boolean createTopic(String kafkaTopicToCreate) {
        boolean success = true;

        // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
        // createTopic() will only seem to work (it will return without error).  The topic will exist in
        // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
        // topic.
        ZkClient zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);

        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), false);

        int partitions = 1;
        int replication = 1;
        Properties topicConfig = new Properties(); // add per-topic configurations settings here

        if (!AdminUtils.topicExists(zkUtils, kafkaTopicToCreate)) {
            AdminUtils.createTopic(zkUtils, kafkaTopicToCreate, partitions, replication, topicConfig, null);
        }
        zkClient.close();

        return success;
    }

    private boolean deleteTopic(String kafkaTopicToDelete) {
        boolean success = false;

        // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
        // createTopic() will only seem to work (it will return without error).  The topic will exist in
        // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
        // topic.
        ZkClient zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);

        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), false);

        if (AdminUtils.topicExists(zkUtils, kafkaTopicToDelete)) {
            AdminUtils.deleteTopic(zkUtils, kafkaTopicToDelete);
        }
        if (!AdminUtils.topicExists(zkUtils, kafkaTopicToDelete)) {
            AdminUtils.deleteTopic(zkUtils, kafkaTopicToDelete);
        }
        zkClient.close();

        return success;
    }
}
