package com.github.lburgazzoli.kafka.jmh;

public class Main {
    public static void main(String[] args) throws Exception {
        BenchmarkState benchmarkState = new BenchmarkState();
        benchmarkState.fuelLimit = 500000;
        benchmarkState.setUp();
        benchmarkState.call();
        benchmarkState.tearDown();
    }
}
