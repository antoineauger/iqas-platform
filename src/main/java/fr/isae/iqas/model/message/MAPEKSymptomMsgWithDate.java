package fr.isae.iqas.model.message;

/**
 * Created by an.auger on 29/03/2017.
 */
public class MAPEKSymptomMsgWithDate {
    private long symptomCreationDate;
    private MAPEKInternalMsg.SymptomMsg symptomMsg;

    public MAPEKSymptomMsgWithDate(MAPEKInternalMsg.SymptomMsg symptomMsg) {
        this.symptomCreationDate = System.currentTimeMillis();
        this.symptomMsg = symptomMsg;
    }

    public long getSymptomCreationDate() {
        return symptomCreationDate;
    }

    public MAPEKInternalMsg.SymptomMsg getSymptomMsg() {
        return symptomMsg;
    }
}
