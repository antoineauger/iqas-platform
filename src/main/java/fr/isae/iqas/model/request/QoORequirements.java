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
    private Map<String, String> additional_params;

    public QoORequirements() {
        this.sla_level = SLALevel.BEST_EFFORT;
        this.operator = Operator.NONE;
        this.interested_in = new ArrayList<>();
        this.additional_params = new ConcurrentHashMap<>();
    }

    /**
     *
     * @param operator is an optional parameter (default: NONE)
     * @param sla_level is an optional parameter (default: BEST_EFFORT)
     * @param interested_in
     * @param additional_params
     */
    @JsonCreator
    public QoORequirements(@JsonProperty("operator") String operator,
                           @JsonProperty("sla_level") String sla_level,
                           @JsonProperty("interested_in") List<String> interested_in,
                           @JsonProperty("additional_params") Map<String, String> additional_params) {
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
        this.additional_params = additional_params;
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

    public Map<String, String> getAdditional_params() {
        return additional_params;
    }

    @Override
    public boolean equals(Object other){
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof QoORequirements)) return false;
        QoORequirements otherMyClass = (QoORequirements) other;
        if (otherMyClass.getInterested_in().size() != this.interested_in.size()) return false;

        for (int i=0 ; i<this.interested_in.size() ; i++) {
            if (!this.interested_in.get(i).equals(otherMyClass.getInterested_in().get(i))) {
                return false;
            }
        }

        return (otherMyClass.getSla_level().equals(this.sla_level)
                && otherMyClass.getOperator().equals(this.operator)
                && otherMyClass.getAdditional_params().entrySet().equals(this.additional_params.entrySet()));
    }
}
