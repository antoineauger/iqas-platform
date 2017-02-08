package fr.isae.iqas.model.quality;

import fr.isae.iqas.model.observation.Information;

import java.sql.Timestamp;

import static fr.isae.iqas.model.quality.QoOAttribute.OBS_ACCURACY;
import static fr.isae.iqas.model.quality.QoOAttribute.OBS_FRESHNESS;

/**
 * Created by an.auger on 08/02/2017.
 */
public class MySpecificQoOAttributeComputation implements IComputeQoOAttributes {
    public MySpecificQoOAttributeComputation() {
        
    }

    @Override
    @QoOParam(name = "lower_bound", type = Double.class)
    @QoOParam(name = "upper_bound", type = Double.class)
    public Information computeQoOAccuracy(Information information, Double lower_bound, Double upper_bound) {
        Double currentValue = information.getValue();
        Double accuracy;

        if (currentValue >= lower_bound  && currentValue <= upper_bound) {
            accuracy = 100.0;
        }
        else {
            Double absoluteDist = 0.0;
            if (currentValue < lower_bound) {
                absoluteDist = lower_bound - currentValue;
            }
            else if (currentValue > upper_bound) {
                absoluteDist = currentValue - upper_bound;
            }
            if (absoluteDist >= (upper_bound - lower_bound)) {
                accuracy = 0.0;
            }
            else {
                accuracy = ((upper_bound - lower_bound) - absoluteDist) / (upper_bound - lower_bound);
            }
        }

        information.setQoOAttribute(OBS_ACCURACY, accuracy);
        return information;
    }

    @Override
    public Information computeQoOFreshness(Information information) {
        Timestamp informationSensingDate = information.getTimestamp();
        Double age = (double) (System.currentTimeMillis() - informationSensingDate.getTime());

        information.setQoOAttribute(OBS_FRESHNESS, age);
        return information;
    }
}
