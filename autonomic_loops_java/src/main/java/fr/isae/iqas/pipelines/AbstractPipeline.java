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
    private boolean adaptable;

    private List<Operator> supportedOperators;
    private Map<String, String> params;
    private Set<String> customizableParams;
    private Map<String, Class> qooParamPrototypes;

    private Map<String, String> qooParams;

    private ActorRef monitorActor;
    private FiniteDuration reportFrequency;
    private IComputeQoOAttributes computeAttributeHelper;

    public AbstractPipeline(String pipelineName, boolean adaptable) {
        this.pipelineName = pipelineName;
        this.adaptable = adaptable;

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
                                            Map<String, String> qooParams) {
        this.computeAttributeHelper = computeAttributeHelper;
        for (String s : qooParams.keySet()) {
            if (qooParamPrototypes.containsKey(s)) {
                this.qooParams.put(s, qooParams.get(s));
            }
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

    public Map<String, String> getQooParams() {
        return qooParams;
    }
}
