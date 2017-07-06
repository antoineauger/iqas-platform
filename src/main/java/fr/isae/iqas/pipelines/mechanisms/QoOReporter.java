package fr.isae.iqas.pipelines.mechanisms;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import fr.isae.iqas.model.message.QoOReportMsg;
import fr.isae.iqas.model.observation.RawData;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

import static fr.isae.iqas.model.quality.QoOAttribute.OBS_ACCURACY;
import static fr.isae.iqas.model.quality.QoOAttribute.OBS_FRESHNESS;

/**
 * Created by an.auger on 05/07/2017.
 */
public class QoOReporter extends GraphStage<FlowShape<RawData, QoOReportMsg>> {

    private final FiniteDuration reportFrequency;
    private final String uniqueID;
    private final String associatedRequestID;

    public QoOReporter(String uniqueID, String associatedRequestID) {
        this.uniqueID = uniqueID;
        this.associatedRequestID = associatedRequestID;
        this.reportFrequency = new FiniteDuration(1, TimeUnit.SECONDS);
    }

    public final Inlet<RawData> in = Inlet.create("QoOReporter.in");
    public final Outlet<QoOReportMsg> out = Outlet.create("QoOReporter.out");
    private final FlowShape<RawData, QoOReportMsg> shape = FlowShape.of(in, out);

    @Override
    public FlowShape<RawData, QoOReportMsg> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
        return new GraphStageLogic(shape) {

            private long lastEmission = 0;

            {

                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() {
                        RawData elem = grab(in);

                        long now = System.currentTimeMillis();
                        if ((now - lastEmission >= reportFrequency.toMillis())) {
                            QoOReportMsg qoOReportAttributes = new QoOReportMsg(uniqueID);
                            qoOReportAttributes.setProducerName(elem.getProducer());
                            qoOReportAttributes.setRequestID(associatedRequestID);
                            qoOReportAttributes.setQooAttribute(OBS_FRESHNESS.toString(), elem.getQoOAttribute(OBS_FRESHNESS));
                            qoOReportAttributes.setQooAttribute(OBS_ACCURACY.toString(), elem.getQoOAttribute(OBS_ACCURACY));

                            push(out, qoOReportAttributes);
                            lastEmission = now;
                        }
                        else {
                            pull(in);
                        }

                    }
                });

                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        pull(in);
                    }
                });

            }
        };
    }
}
