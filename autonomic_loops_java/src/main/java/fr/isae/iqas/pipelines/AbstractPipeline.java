package fr.isae.iqas.pipelines;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.stream.javadsl.Flow;
import fr.isae.iqas.MainClass;
import fr.isae.iqas.model.quality.IComputeQoOAttributes;
import fr.isae.iqas.model.request.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static fr.isae.iqas.model.message.QoOReportMsg.ReportSubject;
import static fr.isae.iqas.model.message.QoOReportMsg.ReportSubject.KEEP_ALIVE;

/**
 * Created by an.auger on 08/02/2017.
 */
public abstract class AbstractPipeline {
    public Logger logger = LoggerFactory.getLogger(MainClass.class);
    final FiniteDuration ONE_SECOND = FiniteDuration.create(1, TimeUnit.SECONDS);

    private String pipelineName;
    private String associatedRequest_id;
    private String tempID;
    private boolean adaptable;

    private List<Operator> supportedOperators;
    private Map<String, String> params;
    private Set<String> customizableParams;
    private Map<String, Class> qooParamPrototypes;

    private Map<String,Map<String, String>> qooParams;

    private ActorRef monitorActor;
    private FiniteDuration reportFrequency;
    private IComputeQoOAttributes computeAttributeHelper;

    public AbstractPipeline(String pipelineName, boolean adaptable) {
        this.pipelineName = pipelineName;
        this.adaptable = adaptable;
        this.tempID = "UNKNOWN";
        this.associatedRequest_id = "UNKNOWN";

        this.params = new ConcurrentHashMap<>();
        this.customizableParams = new HashSet<>();
        this.supportedOperators = Collections.synchronizedList(new ArrayList<Operator>());
        this.qooParamPrototypes = new ConcurrentHashMap<>();
        this.qooParams = new ConcurrentHashMap<>();

        this.monitorActor = null;
        this.reportFrequency = ONE_SECOND;
        this.qooParamPrototypes = IComputeQoOAttributes.getQoOParamsForInterface();
    }

    public void setOptionsForQoOComputation(IComputeQoOAttributes computeAttributeHelper,
                                            Map<String,Map<String, String>> qooParams) {
        this.computeAttributeHelper = computeAttributeHelper;
        for (String topic : qooParams.keySet()) {
            Map<String, String> qooParamForTopic = new ConcurrentHashMap<>();
            for (String param : qooParams.get(topic).keySet()) {
                if (qooParamPrototypes.containsKey(param)) {
                    qooParamForTopic.put(param, qooParams.get(topic).get(param));
                }
            }
            this.qooParams.put(topic, qooParamForTopic);
        }
    }

    public void setOptionsForMAPEKReporting(ActorRef monitorActor,
                                            FiniteDuration reportFrequency) {
        this.monitorActor = monitorActor;
        this.reportFrequency = reportFrequency;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public List<Operator> getSupportedOperators() {
        return supportedOperators;
    }

    public boolean isAdaptable() {
        return adaptable;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public Set<String> getCustomizableParams() {
        return customizableParams;
    }

    public void setParameter(String param, String newValue, boolean customizable) {
        if (customizable) {
            customizableParams.add(param);
        }
        params.put(param, newValue);
    }

    public boolean setCustomizableParameter(String param, String newValue) {
        if (!customizableParams.contains(param)) {
            return false;
        }
        else {
            params.put(param, newValue);
            return true;
        }
    }

    public void addSupportedOperator(Operator operator) {
        supportedOperators.add(operator);
    }

    public ActorRef getMonitorActor() {
        return monitorActor;
    }

    public FiniteDuration getReportFrequency() {
        return reportFrequency;
    }

    public void setReportFrequency(FiniteDuration reportFrequency) {
        this.reportFrequency = reportFrequency;
    }

    public Flow<ReportSubject, Integer, NotUsed> getFlowToComputeObsRate() {
        return Flow.of(ReportSubject.class).keepAlive(reportFrequency.div(2), () -> KEEP_ALIVE)
                .map(p -> {
                    if (p == KEEP_ALIVE) {
                        return 0;
                    }
                    else {
                        return 1;
                    }
                }).groupedWithin(Integer.MAX_VALUE, reportFrequency)
                .map(i -> i.stream().mapToInt(Integer::intValue).sum());
    }

    public IComputeQoOAttributes getComputeAttributeHelper() {
        return computeAttributeHelper;
    }

    public Map<String,Map<String, String>> getQooParams() {
        return qooParams;
    }

    public String getAssociatedRequest_id() {
        return associatedRequest_id;
    }

    public void setAssociatedRequest_id(String associatedRequest_id) {
        this.associatedRequest_id = associatedRequest_id;
    }

    public String getTempID() {
        return tempID;
    }

    public void setTempID(String tempID) {
        this.tempID = tempID;
    }

    public String getUniqueID() {
        return this.pipelineName + "_" + this.tempID;
    }
}
