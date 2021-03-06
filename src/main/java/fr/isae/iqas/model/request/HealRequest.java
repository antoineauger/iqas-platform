package fr.isae.iqas.model.request;

import fr.isae.iqas.model.jsonld.QoOPipeline;
import fr.isae.iqas.model.quality.QoOAttribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by an.auger on 03/05/2017.
 */
public class HealRequest {
    private String concernedRequest;
    private String uniqueIDPipeline;
    private QoOAttribute concernedAttr;
    private long healStartDate;
    private long observeUntil;
    private long observeDuration;
    private int retries;
    private boolean isHealing;

    // To emulate a kind of memory
    private List<QoOAttribute> healFor;
    private List<QoOPipeline> triedRemedies;
    private List<Map<String, String>> paramsForRemedies;

    public HealRequest(String concernedRequest, String uniqueIDPipeline, long observeDuration) {
        this.concernedRequest = concernedRequest;
        this.uniqueIDPipeline = uniqueIDPipeline;
        this.retries = 0;
        this.isHealing = false;
        this.observeDuration = observeDuration;

        this.healFor = new ArrayList<>();
        this.triedRemedies = new ArrayList<>();
        this.paramsForRemedies = new ArrayList<>();
    }

    public boolean canPerformHeal() {
        long now = System.currentTimeMillis();
        return !isHealing || now >= observeUntil;
    }

    public void performHeal(QoOAttribute concernedAttr, QoOPipeline healPipelineToApply, Map<String, String> params) {
        this.concernedAttr = concernedAttr;
        healFor.add(concernedAttr);
        triedRemedies.add(healPipelineToApply);
        paramsForRemedies.add(params);

        healStartDate = System.currentTimeMillis();
        observeUntil = System.currentTimeMillis() + observeDuration;
        retries += 1;
        isHealing = true;
    }

    public boolean hasAlreadyBeenTried(QoOAttribute qoOAttribute, QoOPipeline healPipeline) {
        for (int i=0 ; i<healFor.size() ; i++) {
            if (healFor.get(i).equals(qoOAttribute) && triedRemedies.get(i).pipeline.equals(healPipeline.pipeline)) {
                return true;
            }
        }
        return false;
    }

    public QoOPipeline getLastTriedRemedy() {
        if (triedRemedies.size() > 0) {
            return triedRemedies.get(triedRemedies.size()-1);
        }
        else {
            return null;
        }
    }

    public QoOAttribute getLastHealFor() {
        if (healFor.size() > 0) {
            return healFor.get(healFor.size()-1);
        }
        else {
            return null;
        }
    }

    public Map<String, String> getLastParamsForRemedies() {
        if (paramsForRemedies.size() > 0) {
            return paramsForRemedies.get(paramsForRemedies.size()-1);
        }
        else {
            return null;
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

    public long getHealStartDate() {
        return healStartDate;
    }

    public String getUniqueIDPipeline() {
        return uniqueIDPipeline;
    }
}
