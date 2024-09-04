package com.github.lburgazzoli.kafka.jmh;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.extism.sdk.ExtismCurrentPlugin;
import org.extism.sdk.ExtismFunction;
import org.extism.sdk.HostFunction;
import org.extism.sdk.HostUserData;
import org.extism.sdk.LibExtism;

import com.fasterxml.jackson.databind.ObjectMapper;

public class HostFunctionStubs {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private byte[] key;
    private byte[] value;

    public void setRecord(String key, String value) {
        this.key = key.getBytes(StandardCharsets.UTF_8);
        this.value = value.getBytes(StandardCharsets.UTF_8);
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public HostFunction[] imports() {
        LibExtism.ExtismValType[] void_t = new LibExtism.ExtismValType[0];
        LibExtism.ExtismValType[] i64_t = { LibExtism.ExtismValType.I64 };
        return new HostFunction[] {
                hostFunction("get_key", this::getKeyFn, void_t, i64_t),
                hostFunction("set_key", this::setKeyFn, i64_t, void_t),
                hostFunction("get_value", this::getValueFn, void_t, i64_t),
                hostFunction("set_value", this::setValueFn, i64_t, void_t),
                hostFunction("get_header", this::nop, i64_t, i64_t),
                hostFunction("set_header", this::nop, i64_t, void_t),
                hostFunction("get_topic", this::nop, void_t, i64_t),
                hostFunction("set_topic", this::nop, i64_t, void_t),
                hostFunction("get_record", this::nop, void_t, i64_t),
                hostFunction("set_record", this::nop, i64_t, void_t),
        };
    }

    private void nop(ExtismCurrentPlugin extismCurrentPlugin, LibExtism.ExtismVal[] extismVals,
        LibExtism.ExtismVal[] extismVals1, Optional<HostUserData> hostUserData) {
    }

    private void getValueFn(ExtismCurrentPlugin plugin, LibExtism.ExtismVal[] params, LibExtism.ExtismVal[] returns,
        Optional<HostUserData> userData) {
        plugin.returnBytes(returns[0], this.value);
    }

    private void setValueFn(ExtismCurrentPlugin plugin, LibExtism.ExtismVal[] args, LibExtism.ExtismVal[] returns,
        Optional<HostUserData> userData) {
        byte[] bytes = plugin.inputBytes(args[0]);
        this.value = bytes;
    }

    private void getKeyFn(ExtismCurrentPlugin plugin, LibExtism.ExtismVal[] params, LibExtism.ExtismVal[] returns,
        Optional<HostUserData> userData) {
        plugin.returnBytes(returns[0], this.key);
    }

    private void setKeyFn(ExtismCurrentPlugin plugin, LibExtism.ExtismVal[] args, LibExtism.ExtismVal[] returns,
        Optional<HostUserData> userData) {
        byte[] bytes = plugin.inputBytes(args[0]);
        this.key = bytes;
    }

    private static HostFunction hostFunction(String name, ExtismFunction<?> wasmFunction, LibExtism.ExtismValType[] inType,
        LibExtism.ExtismValType[] outType) {
        return new HostFunction(name, inType, outType, wasmFunction, Optional.empty());
    }

}
