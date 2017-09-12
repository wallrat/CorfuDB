package org.corfudb.benchmark;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;
import org.corfudb.runtime.CorfuRuntime;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.State;

import static org.openjdk.jmh.annotations.Scope.Benchmark;

@State(Benchmark)
public class Common {
    final BitsStreamGenerator RND = new MersenneTwister();

    CorfuRuntime rt;

    @Param("1000")
    int num;

    public void setup() {
        rt = new CorfuRuntime("localhost:9000").connect();
    }

    public void teardown() {
        rt.shutdown();
    }
}
