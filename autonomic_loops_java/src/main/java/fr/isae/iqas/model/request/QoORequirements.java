package fr.isae.iqas.model.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import fr.isae.iqas.model.quality.QoOAttribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    @JsonCreator
    public QoORequirements(@JsonProperty("operator") String operator,
                           @JsonProperty("sla_level") String sla_level,
                           @JsonProperty("interested_in") List<String> interested_in,
                           @JsonProperty("additional_params") Map<String, String> additional_params) {
        this.operator = Operator.valueOf(operator);
        this.sla_level = SLALevel.valueOf(sla_level);
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
}
