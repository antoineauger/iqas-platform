package fr.isae.iqas.model.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import fr.isae.iqas.model.quality.QoOAttribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 24/02/2017.
 */
public class QoORequirements {
    public enum SLALevel {
        BEST_EFFORT,
        GUARANTEED
    }

    private List<QoOAttribute> interested_in;
    private Operator operator;
    private SLALevel sla_level;
    private Map<String, String> iqas_params;
    private Map<String, String> custom_params;

    public QoORequirements() {
        this.sla_level = SLALevel.BEST_EFFORT;
        this.operator = Operator.NONE;
        this.interested_in = new ArrayList<>();
        this.custom_params = new ConcurrentHashMap<>();
        this.iqas_params = new ConcurrentHashMap<>();
    }

    /**
     * @param operator is an optional parameter (default: NONE)
     * @param sla_level is an optional parameter (default: BEST_EFFORT)
     * @param interested_in
     * @param iqas_params
     * @param custom_params
     */
    @JsonCreator
    public QoORequirements(@JsonProperty("operator") String operator,
                           @JsonProperty("sla_level") String sla_level,
                           @JsonProperty("interested_in") List<String> interested_in,
                           @JsonProperty("iqas_params") Map<String, String> iqas_params,
                           @JsonProperty("custom_params") Map<String, String> custom_params) {
        if (operator == null) {
            this.operator = Operator.NONE;
        }
        else {
            this.operator = Operator.valueOf(operator);
        }

        if (sla_level == null) {
            this.sla_level = SLALevel.BEST_EFFORT;
        }
        else {
            this.sla_level = SLALevel.valueOf(sla_level);
        }

        this.interested_in = new ArrayList<>();
        for (String s : interested_in) {
            this.interested_in.add(QoOAttribute.valueOf(s));
        }

        this.custom_params = new ConcurrentHashMap<>();
        if (custom_params != null) {
            this.custom_params.putAll(custom_params);
        }

        this.iqas_params = new ConcurrentHashMap<>();
        if (iqas_params != null) {
            this.iqas_params.putAll(iqas_params);
        }
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public QoORequirements.SLALevel getSla_level() {
        return sla_level;
    }

    public void setSla_level(QoORequirements.SLALevel sla_level) {
        this.sla_level = sla_level;
    }

    public List<QoOAttribute> getInterested_in() {
        return interested_in;
    }

    public void setInterested_in(List<QoOAttribute> interested_in) {
        this.interested_in = interested_in;
    }

    public Map<String, String> getCustom_params() {
        return custom_params;
    }

    public Map<String, String> getIqas_params() {
        return iqas_params;
    }

    public void setIqas_params(Map<String, String> iqas_params) {
        this.iqas_params = iqas_params;
    }

    @Override
    public boolean equals(Object other){
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof QoORequirements)) return false;
        QoORequirements otherMyClass = (QoORequirements) other;
        if (otherMyClass.getInterested_in().size() != this.interested_in.size()) return false;

        if (otherMyClass.getCustom_params().size() != this.custom_params.size()
                || otherMyClass.getIqas_params().size() != this.iqas_params.size()) {
            return false;
        }
        else if (otherMyClass.getCustom_params().size() == 0 && this.custom_params.size() > 0
                || otherMyClass.getCustom_params().size() >= 0 && this.custom_params.size() == 0
                || otherMyClass.getIqas_params().size() == 0 && this.iqas_params.size() > 0
                || otherMyClass.getIqas_params().size() > 0 && this.iqas_params.size() == 0) {
            return false;
        }

        for (int i=0 ; i<this.interested_in.size() ; i++) {
            if (!this.interested_in.get(i).equals(otherMyClass.getInterested_in().get(i))) {
                return false;
            }
        }

        return (otherMyClass.getSla_level().equals(this.sla_level)
                && otherMyClass.getOperator().equals(this.operator)
                && otherMyClass.getCustom_params().entrySet().equals(this.custom_params.entrySet())
                && otherMyClass.getIqas_params().entrySet().equals(this.iqas_params.entrySet()));
    }
}
