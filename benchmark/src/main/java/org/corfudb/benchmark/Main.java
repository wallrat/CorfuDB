package org.corfudb.benchmark;

import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CorfuTableBenchmark.class.getSimpleName())
                .forks(1)
                .warmupIterations(3)
                .timeUnit(TimeUnit.MILLISECONDS)
                .measurementIterations(3)
                .mode(Mode.AverageTime)
                .mode(Mode.Throughput)
                .build();
        new Runner(opt).run();
    }
}