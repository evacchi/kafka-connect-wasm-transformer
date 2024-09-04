package com.github.lburgazzoli.kafka.jmh;

import java.nio.file.Path;
import java.util.List;

import org.extism.sdk.Plugin;
import org.extism.sdk.manifest.Manifest;
import org.extism.sdk.wasm.PathWasmSource;
import org.extism.sdk.wasm.WasmSourceResolver;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class BenchmarkState {
    private static String modulePath = "src/jmh/resources/plugin.wasm";

    @Param({ "2500000" }) // 500000 * 5 iterations.
    public long fuelLimit;

    private Plugin plugin;
    private HostFunctionStubs hostFunctions;

    @Setup(Level.Iteration)
    public void setUp() {
        WasmSourceResolver wasmSourceResolver = new WasmSourceResolver();
        PathWasmSource wasmSource = wasmSourceResolver.resolve(Path.of(modulePath));

        Manifest manifest = new Manifest(List.of(wasmSource), null);

        this.hostFunctions = new HostFunctionStubs();
        if (fuelLimit < 0) {
            // No fuel limit.
            this.plugin = new Plugin(manifest, true, hostFunctions.imports());
        } else {
            this.plugin = new Plugin(manifest, true, hostFunctions.imports(), fuelLimit);
        }

        hostFunctions.setRecord("the-key", "the-value");
    }

    public void call() {
        this.plugin.call("to_upper", new byte[0]);
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        this.plugin.close();
    }

}
