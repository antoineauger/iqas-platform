package fr.isae.iqas.model.message;

/**
 * Created by an.auger on 14/09/2016.
 */
public class RFCMsg {
    private String remedyToPlan;

    public RFCMsg(String remedyToPlan) {
        this.remedyToPlan = remedyToPlan;
    }

    public String getRemedyToPlan() {
        return remedyToPlan;
    }
}
