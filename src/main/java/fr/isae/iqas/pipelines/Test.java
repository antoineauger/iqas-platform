package fr.isae.iqas.pipelines;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.*;
import scala.concurrent.duration.FiniteDuration;

/**
 * Created by an.auger on 30/04/2017.
 */
public class Test<A> extends GraphStage<FlowShape<A, A>> {

    private final FiniteDuration silencePeriod;

    public Test(FiniteDuration silencePeriod) {
        this.silencePeriod = silencePeriod;
    }

    public final Inlet<A> in = Inlet.create("TimedGate.in");
    public final Outlet<A> out = Outlet.create("TimedGate.out");

    private final FlowShape<A, A> shape = FlowShape.of(in, out);
    @Override
    public FlowShape<A, A> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new TimerGraphStageLogic(shape) {

            private boolean open = false;
            private int countElements = 0;

            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        A elem = grab(in);
                        if (open) {
                            pull(in);
                        }
                        else {
                            push(out, elem);
                            open = true;
                            scheduleOnce("key", silencePeriod);
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

            @Override
            public void onTimer(Object key) {
                if (key.equals("key")) {
                    open = false;
                }
            }
        };
    }
}
