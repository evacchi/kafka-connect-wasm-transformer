package com.github.lburgazzoli.kafka.jmh;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;

public class BenchmarkRunner {

    @Benchmark
    @Fork(warmups = 1)
    @Measurement(iterations = 5)
    @BenchmarkMode(Mode.Throughput)
    public void run(BenchmarkState state) {
        state.call();
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
