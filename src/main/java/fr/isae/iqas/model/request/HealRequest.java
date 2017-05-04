package fr.isae.iqas.model.request;

import fr.isae.iqas.model.jsonld.QoOPipeline;
import fr.isae.iqas.model.quality.QoOAttribute;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by an.auger on 03/05/2017.
 */
public class HealRequest {
    private String concernedRequest;
    private QoOAttribute concernedAttr;
    private Timestamp healStartDate;
    private Timestamp observeUntil;
    private long observeDuration;
    private int retries;
    private boolean isHealing;

    // To emulate a kind of memory
    private List<QoOAttribute> healFor;
    private List<QoOPipeline> triedRemedies;
    private List<Map<String, String>> paramForRemedies;

    public HealRequest(String concernedRequest, long observeDuration) {
        this.concernedRequest = concernedRequest;
        this.retries = 0;
        this.isHealing = false;
        this.observeDuration = observeDuration;

        this.healFor = new ArrayList<>();
        this.triedRemedies = new ArrayList<>();
        this.paramForRemedies = new ArrayList<>();
    }

    public boolean canPerformHeal() {
        long now = System.currentTimeMillis();
        return !isHealing || now >= observeUntil.getTime();
    }

    public boolean performHeal(QoOAttribute concernedAttr, QoOPipeline healPipelineToApply, Map<String, String> params) {
        this.concernedAttr = concernedAttr;
        healFor.add(concernedAttr);
        triedRemedies.add(healPipelineToApply);
        paramForRemedies.add(params);

        long now = System.currentTimeMillis();
        if (!isHealing || now >= observeUntil.getTime()) {
            healStartDate = new Timestamp(now);
            observeUntil = new Timestamp(now + observeDuration);
            retries += 1;
            return true;
        }
        else {
            return false;
        }
    }

    public QoOAttribute getConcernedAttr() {
        return concernedAttr;
    }

    public int getRetries() {
        return retries;
    }

    public String getConcernedRequest() {
        return concernedRequest;
    }
}
