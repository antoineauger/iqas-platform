package fr.isae.iqas.model.message;

/**
 * Created by an.auger on 21/02/2017.
 */
public class MAPEKInternalMsg {

    public static class RFCMsg {
        private String remedyToPlan;

        public RFCMsg(String remedyToPlan) {
            this.remedyToPlan = remedyToPlan;
        }

        public String getRemedyToPlan() {
            return remedyToPlan;
        }
    }
    
}
