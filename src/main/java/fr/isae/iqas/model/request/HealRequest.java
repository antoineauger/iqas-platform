package fr.isae.iqas.model.request;

import fr.isae.iqas.model.message.MAPEKInternalMsg;
import fr.isae.iqas.model.quality.QoOAttribute;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

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
    private List<MAPEKInternalMsg.ActionMsg> triedRemedies;

    public HealRequest(String concernedRequest, QoOAttribute concernedAttr, long observeDuration) {
        this.concernedRequest = concernedRequest;
        this.concernedAttr = concernedAttr;
        this.retries = 0;
        this.isHealing = false;
        this.triedRemedies = new ArrayList<>();
        this.observeDuration = observeDuration;
    }

    public boolean canPerformHeal() {
        long now = System.currentTimeMillis();
        return !isHealing || now >= observeUntil.getTime();
    }

    public boolean performHeal(QoOAttribute concernedAttr) {
        this.concernedAttr = concernedAttr;
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
}
