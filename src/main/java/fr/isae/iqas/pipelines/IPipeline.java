package fr.isae.iqas.pipelines;

import akka.actor.ActorRef;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import fr.isae.iqas.model.observation.ObservationLevel;
import fr.isae.iqas.model.quality.IComputeQoOAttributes;
import fr.isae.iqas.model.request.Operator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.concurrent.duration.FiniteDuration;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by an.auger on 31/01/2017.
 */

@JsonIgnoreProperties({"logger", "monitorActor", "flowToComputeObsRate", "computeAttributeHelper", "pipelineName",
        "pipelineID", "params", "qooParams", "reportFrequency", "associatedRequest_id",
        "tempID", "uniqueID", "topicToPublish", "pipelineGraph", "askedLevel", "operatorToApply"})
public interface IPipeline {
    ActorRef getMonitorActor();

    void setOptionsForQoOComputation(IComputeQoOAttributes computeAttributeHelper,
                                     Map<String, Map<String, String>> qooParams);

    void setOptionsForMAPEKReporting(ActorRef monitorActor,
                                     FiniteDuration reportFrequency);

    /**
     *
     * @param topicToPublish the name of the topic to publish to
     * @param askedLevel
     * @param operatorToApply
     */
    void setupPipelineGraph(String topicToPublish, ObservationLevel askedLevel, Operator operatorToApply);

    /**
     * Ground method to get the Akka graph to run
     * It corresponds to a pipeline in the iQAS platform, composed of none, one or more QoO mechanisms
     * @return the graph to run for the given pipeline
     */

    Graph<FlowShape<ConsumerRecord<byte[], String>, ProducerRecord<byte[], String>>, Materializer> getPipelineGraph();

    /**
     * @return the Simple class name (e.g., OutputPipeline)
     */
    String getPipelineID();

    /**
     * @return a readable String representing the pipeline name (e.g., Output Pipeline)
     */
    String getPipelineName();

    /**
     * @return a list of all supported Operator
     * @see Operator for all possibilites
     */
    @JsonProperty("supported_operators")
    List<Operator> getSupportedOperators();

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
    Map<String, String> getParams();

    /**
     * Get only the name of the customizable parameters
     * @return List(String) of the parameter names
     */
    @JsonProperty("customizable_params")
    Set<String> getCustomizableParams();

    /**
     * Useful to set a specific parameter
     * @param param the default param to set
     * @param value the new value casted in String
     * @return true if the operation has succeeded, false otherwise
     */
    boolean setCustomizableParameter(String param, String value);

    Map<String,Map<String, String>> getQooParams();

    String getAssociatedRequest_id();

    void setAssociatedRequestID(String associatedRequest_id);

    String getUniqueID();

    void setTempID(String tempID);

    String getTempID();

    ObservationLevel getAskedLevel();

    void setAskedLevel(ObservationLevel askedLevel);
}
