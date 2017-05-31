package fr.isae.iqas.pipelines.mechanisms;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;

import java.util.Collections;

/**
 * This QoO mechanism allows to clone n times each incoming observations and therefore to emit the same n observation per second.
 *
 * @param <T> the type of the Objects to pull/push
 */
public class CloneSameValueGS<T> extends GraphStage<FlowShape<T, T>> {

    private int nbCopiesToMake;

    public CloneSameValueGS(int nbCopiesToMake) {
        this.nbCopiesToMake = nbCopiesToMake;
    }

    public final Inlet<T> in = Inlet.create("CloneSameValue.in");
    public final Outlet<T> out = Outlet.create("CloneSameValue.out");
    private final FlowShape<T, T> shape = FlowShape.of(in, out);

    @Override
    public FlowShape<T, T> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape) {

            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        T elem = grab(in);
                        // this will temporarily suspend this handler until the two elems
                        // are emitted and then reinstates it
                        emitMultiple(out, Collections.nCopies(nbCopiesToMake, elem).iterator());
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
