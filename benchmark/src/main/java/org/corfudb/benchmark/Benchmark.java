package org.corfudb.benchmark;

import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Benchmark {
    private static final String doc = "Benchmark.\n"
            + "\n"
            + "Usage:\n"
            + "  benchmark <port>\n"
            + "\n";

    public static void main(String[] args) throws RunnerException {

        Map<String, Object> opts = new Docopt(doc).withVersion(GitRepositoryState.getRepositoryState().describe)
                .parse(args);

        Options opt = new OptionsBuilder()
                .include(CorfuTableBenchmark.class.getSimpleName())
                .forks(1)
                .warmupIterations(3)
                .timeUnit(TimeUnit.MILLISECONDS)
                .param("port", (String) opts.get("<port>"))
                .measurementIterations(3)
                .mode(Mode.AverageTime)
                .mode(Mode.Throughput)
                .build();
        new Runner(opt).run();
    }
}