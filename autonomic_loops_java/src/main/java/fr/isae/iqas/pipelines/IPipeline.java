package fr.isae.iqas.pipelines;

import akka.Done;
import akka.kafka.javadsl.Consumer;
import akka.stream.ClosedShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Created by an.auger on 31/01/2017.
 */

@JsonIgnoreProperties({"pipelineName", "pipelineID"})
public interface IPipeline {

    /**
     * Ground method to get the Akka graph to run
     * It corresponds to a pipeline in the iQAS platform, composed of none, one or more QoO mechanisms
     * @param kafkaSource the kafka consumer to use
     * @param kafkaSink the kafka producer to use
     * @param topicToPublish the name of the topic to publish to
     * @return the graph to run for the given pipeline
     */

    Graph<ClosedShape, Materializer> getPipelineGraph(Source<ConsumerRecord<byte[], String>, Consumer.Control> kafkaSource,
                                                      Sink<ProducerRecord, CompletionStage<Done>> kafkaSink,
                                                      String topicToPublish);

    /**
     * @return the pipeline ID corresponding to the Class name .class
     */
    String getPipelineID();

    /**
     * @return a String representing the pipeline name
     */
    String getPipelineName();

    /**
     * Tells if the setParams method can be called for the given pipeline
     * @return a true or false boolean
     */
    @JsonProperty("is_adaptable")
    boolean isAdaptable();

    /**
     * Get the value of the current enforced parameters
     * @return Map(String, String) following the format (key, value)
     */
    @JsonProperty("default_params")
    Map<String, String> getDefaultParams();

    /**
     * Get only the name of the customizable parameters
     * @return List(String) of the parameter names
     */
    @JsonProperty("customizable_params")
    List<String> getCustomizableParams();

    /**
     * Useful to set one or more customizable parameters
     * @param newParams Map(String, String) following the format (key, value)
     * @return true if the operation has succeeded, false otherwise
     */
    boolean setParams(Map<String, String> newParams);

}
