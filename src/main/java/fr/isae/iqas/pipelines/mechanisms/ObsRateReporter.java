package fr.isae.iqas.pipelines.mechanisms;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import fr.isae.iqas.model.message.ObsRateReportMsg;
import fr.isae.iqas.model.observation.RawData;
import scala.concurrent.duration.FiniteDuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by an.auger on 05/07/2017.
 */
public class ObsRateReporter extends GraphStage<FlowShape<RawData, ObsRateReportMsg>> {

    private final FiniteDuration reportFrequency;
    private final String uniqueID;
    private Map<String,Integer> obsRateByTopic;

    public ObsRateReporter(String uniqueID, FiniteDuration reportFrequency) {
        this.uniqueID = uniqueID;
        this.reportFrequency = reportFrequency;
        this.obsRateByTopic = new ConcurrentHashMap<>();
    }

    public final Inlet<RawData> in = Inlet.create("ObsRateReporter.in");
    public final Outlet<ObsRateReportMsg> out = Outlet.create("ObsRateReporter.out");
    private final FlowShape<RawData, ObsRateReportMsg> shape = FlowShape.of(in, out);

    @Override
    public FlowShape<RawData, ObsRateReportMsg> shape() {
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
                        obsRateByTopic.putIfAbsent(elem.getProducer(), 0);
                        obsRateByTopic.put(elem.getProducer(), obsRateByTopic.get(elem.getProducer()) + 1);

                        long now = System.currentTimeMillis();
                        if ((now - lastEmission >= reportFrequency.toMillis())) {
                            ObsRateReportMsg obsRateReportMsg = new ObsRateReportMsg(uniqueID);
                            obsRateReportMsg.setObsRateByTopic(obsRateByTopic);
                            push(out, obsRateReportMsg);
                            obsRateByTopic.clear();
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
