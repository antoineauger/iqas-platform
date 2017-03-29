package fr.isae.iqas.model.message;

import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 29/03/2017.
 */
public class MAPEKSymptomMsgWithDate {
    private FiniteDuration symptomCreationDate;
    private MAPEKInternalMsg.SymptomMsg symptomMsg;

    public MAPEKSymptomMsgWithDate(MAPEKInternalMsg.SymptomMsg symptomMsg) {
        this.symptomCreationDate = new FiniteDuration(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        this.symptomMsg = symptomMsg;
    }

    public FiniteDuration getSymptomCreationDate() {
        return symptomCreationDate;
    }

    public MAPEKInternalMsg.SymptomMsg getSymptomMsg() {
        return symptomMsg;
    }
}
