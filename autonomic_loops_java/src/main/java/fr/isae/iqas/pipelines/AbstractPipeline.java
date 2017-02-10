package fr.isae.iqas.pipelines;

import fr.isae.iqas.model.request.Operator;
import fr.isae.iqas.model.quality.IComputeQoOAttributes;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 08/02/2017.
 */
public abstract class AbstractPipeline {

    private String pipelineName;
    private boolean adaptable;

    private List<Operator> supportedOperators;
    private Map<String, String> params;
    private Set<String> customizableParams;
    private Map<String, Class> qooParamPrototypes;
    private Map<String, String> qooParams;

    private IComputeQoOAttributes computeAttributeHelper;

    public AbstractPipeline(String pipelineName, boolean adaptable) {
        this.pipelineName = pipelineName;
        this.adaptable = adaptable;

        this.params = new ConcurrentHashMap<>();
        this.customizableParams = new HashSet<>();
        this.supportedOperators = Collections.synchronizedList(new ArrayList<Operator>());
        this.qooParamPrototypes = new ConcurrentHashMap<>();
        this.qooParams = new ConcurrentHashMap<>();

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

}
