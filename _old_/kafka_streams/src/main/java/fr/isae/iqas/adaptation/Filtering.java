package fr.isae.iqas.adaptation;

import fr.isae.iqas.adaptation.utils.GenericAvroDeserializer;
import fr.isae.iqas.adaptation.utils.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Filtering {

    public static void main(String[] args) throws Exception {
        Logger log = LoggerFactory.getLogger(Filtering.class);

        String topicToConsume = "";
        String topicToPublish = "";
        int mode = 1;
        double threshold = 0.0;
        double otherThreshold = 0.0;

        // Check for supplied parameters (see Usage)
        if (args.length >= 4) {
            try {
                mode = Integer.parseInt(args[2]);
                threshold = Double.parseDouble(args[3]);
                if (mode == 3) {
                    if (args.length != 5) {
                        printUsage();
                        System.exit(1);
                    }
                    otherThreshold = Double.parseDouble(args[4]);
                }
            } catch (NumberFormatException e) {
                System.err.println("==== ERROR: Unable to correctly parse some arguments ====");
                printUsage();
                System.exit(1);
            }

            topicToConsume = args[0];
            topicToPublish = args[1];
            if (topicToConsume.equals(topicToPublish)) {
                System.err.println("==== ERROR: Topics' names should be different ====");
                printUsage();
                System.exit(1);
            }
        }
        else {
            printUsage();
            System.exit(1);
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Map<String, List<PartitionInfo>> topics = consumer.listTopics();

        if (!topics.containsKey(topicToConsume)) {
            //TODO add topic ? OK but with what parameters ?
            System.out.println("Topic to consume '" + topicToConsume + "' do not exist");
        }
        if (!topics.containsKey(topicToPublish)) {
            //TODO add topic ? OK but with what parameters ?
            System.out.println("Topic to publish '" + topicToPublish + "' do not exist");
        }

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, GenericRecord> views = builder.stream(topicToConsume);
        double finalThreshold = threshold;
        double finalOtherThreshold = otherThreshold;

        KStream<String, GenericRecord> viewsByUser;
        switch (mode) {
            case 1:
                viewsByUser = views.filter((dummy, record) -> (double) record.get("value") <= finalThreshold).through(topicToPublish);
                break;
            case 2:
                viewsByUser = views.filter((dummy, record) -> (double) record.get("value") >= finalThreshold).through(topicToPublish);
                break;
            case 3:
                viewsByUser = views.filter((dummy, record) ->
                        (double) record.get("value") >= finalThreshold && (double) record.get("value") <= finalOtherThreshold)
                        .through(topicToPublish);
                break;
            default:
                System.err.println("==== ERROR: Unknown mode " + String.valueOf(mode) + " ====");
                printUsage();
                System.exit(1);
        }

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close()));
    }

    private static void printUsage() {
        System.err.println("Usage: java ... fr.isae.iqas.adaptation.Filtering TOPIC-TO-CONSUME TOPIC-TO-PUBLISH MODE VALUE [OTHER-VALUE]");
        System.err.println("TOPIC-TO-CONSUME: Name of the topic to pull from.");
        System.err.println("TOPIC-TO-PUBLISH: Name of the topic to publish to.");
        System.err.println("MODE: 1=lower or equal than | 2=greater or equal than | 3=between (included bounds).");
        System.err.println("VALUE: Custom threshold. In mode 'between', this value is the lower bound.");
        System.err.println("[OTHER-VALUE]: (Optional) Only required in mode 'between', max bound.");
    }
}
