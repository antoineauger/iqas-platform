package fr.isae.iqas.pipelines;

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
    private Properties prop;

    public KafkaAdminActor(Properties prop) {
        this.prop = prop;
    }

    @Override
    public void onReceive(Object message) throws Throwable {

    }

    public boolean addKafkaTopic() {
        boolean success = true;

        String zookeeperConnect = prop.getProperty("zookeeper_endpoint_address") + ":" + prop.getProperty("zookeeper_endpoint_port");
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;
        // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
        // createTopic() will only seem to work (it will return without error).  The topic will exist in
        // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
        // topic.
        ZkClient zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);

        // Security for Kafka was added in Kafka 0.9.0.0
        boolean isSecureKafkaCluster = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);

        String topic = "my-topic";
        int partitions = 1;
        int replication = 1;
        Properties topicConfig = new Properties(); // add per-topic configurations settings here
        AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig, null);
        zkClient.close();

        return success;
    }
}
