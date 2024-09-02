package com.github.lburgazzoli.kafka.transformer.wasm;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.extism.sdk.ExtismCurrentPlugin;
import org.extism.sdk.HostFunction;
import org.extism.sdk.HostUserData;
import org.extism.sdk.LibExtism;
import org.extism.sdk.Plugin;
import org.extism.sdk.manifest.Manifest;
import org.extism.sdk.wasm.PathWasmSource;
import org.extism.sdk.wasm.WasmSourceResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dylibso.chicory.runtime.exceptions.WASMMachineException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

public class WasmFunction<R extends ConnectRecord<R>> implements AutoCloseable, Function<R, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WasmFunction.class);

    public static final ObjectMapper MAPPER = JsonMapper.builder().build();

    public static final String MODULE_NAME = "env";
    public static final String FN_ALLOC = "alloc";
    public static final String FN_DEALLOC = "dealloc";

    private final WasmRecordConverter<R> recordConverter;
    private final String functionName;
    private final AtomicReference<R> ref;
    private final Plugin plugin;

    public WasmFunction(
        String modulePath,
        String functionName,
        Converter keyConverter,
        Converter valueConverter,
        HeaderConverter headerConverter) {

        Objects.requireNonNull(modulePath);
        this.ref = new AtomicReference<>();
        this.recordConverter = new WasmRecordConverter<>(keyConverter, valueConverter, headerConverter);
        this.functionName = Objects.requireNonNull(functionName);

        WasmSourceResolver wasmSourceResolver = new WasmSourceResolver();
        PathWasmSource wasmSource = wasmSourceResolver.resolve(Path.of(modulePath));
        Manifest manifest = new Manifest(wasmSource);
        this.plugin = new Plugin(manifest, true, imports());
    }

    ObjectMapper om = new ObjectMapper();

    @Override
    public R apply(R record) {
        try {
            ref.set(record);
            plugin.call(functionName, (byte[]) null);
            return ref.get();
        } catch (WASMMachineException e) {
            LOGGER.warn("message: {}, stack {}", e.getMessage(), e.stackFrames());
            throw new WasmFunctionException(functionName, e);
        } catch (Exception e) {
            throw new WasmFunctionException(functionName, e);
        } finally {
            ref.set(null);
        }
    }

    @Override
    public void close() throws Exception {
        plugin.close();
    }

    private static boolean isError(int number) {
        return (number & (1 << 31)) != 0;
    }

    private static int errSize(int number) {
        return number & (~(1 << 31));
    }

    private HostFunction[] imports() {
        return new HostFunction[] {
                new HostFunction(
                    "get_key",
                    new LibExtism.ExtismValType[0],
                    new LibExtism.ExtismValType[] { LibExtism.ExtismValType.I64 },
                    this::getKeyFn,
                    Optional.empty()),
                new HostFunction(
                    "set_key",
                    new LibExtism.ExtismValType[] { LibExtism.ExtismValType.I64 },
                    new LibExtism.ExtismValType[0],
                    this::setKeyFn,
                    Optional.empty()),
                new HostFunction(
                    "get_value",
                    new LibExtism.ExtismValType[0],
                    new LibExtism.ExtismValType[] { LibExtism.ExtismValType.I64 },
                    this::getValueFn,
                    Optional.empty()),
                new HostFunction(
                    "set_value",
                    new LibExtism.ExtismValType[] { LibExtism.ExtismValType.I64 },
                    new LibExtism.ExtismValType[0],
                    this::setValueFn,
                    Optional.empty()),
                new HostFunction(
                    "get_header",
                    new LibExtism.ExtismValType[] { LibExtism.ExtismValType.I64 },
                    new LibExtism.ExtismValType[] { LibExtism.ExtismValType.I64 },
                    this::getHeaderFn,
                    Optional.empty()),
                new HostFunction(
                    "set_header",
                    new LibExtism.ExtismValType[] { LibExtism.ExtismValType.I64 },
                    new LibExtism.ExtismValType[0],
                    this::setHeaderFn,
                    Optional.empty()),
                new HostFunction(
                    "get_topic",
                    new LibExtism.ExtismValType[0],
                    new LibExtism.ExtismValType[] { LibExtism.ExtismValType.I64 },
                    this::getTopicFn,
                    Optional.empty()),
                new HostFunction(
                    "set_topic",
                    new LibExtism.ExtismValType[] { LibExtism.ExtismValType.I64 },
                    new LibExtism.ExtismValType[0],
                    this::setTopicFn,
                    Optional.empty()),

        };
    }

    //
    // Functions
    //
    // Memory must be de-allocated by the Wasm Module
    //

    //
    // Headers
    //

    private void getHeaderFn(ExtismCurrentPlugin plugin, LibExtism.ExtismVal[] params, LibExtism.ExtismVal[] returns,
        Optional<HostUserData> userData) {
        final R record = this.ref.get();
        final byte[] rawData = recordConverter.fromConnectHeader(record, plugin.inputString(params[0]));
        plugin.returnBytes(returns[0], rawData);
    }

    private void setHeaderFn(ExtismCurrentPlugin plugin, LibExtism.ExtismVal[] params, LibExtism.ExtismVal[] returns,
        Optional<HostUserData> userData) {
        try {
            JsonNode json = MAPPER.readTree(plugin.inputBytes(params[0]));
            String headerName = json.get("key").asText();
            String headerData = json.get("value").asText();

            final R record = this.ref.get();
            final SchemaAndValue sv = recordConverter.toConnectHeader(record, headerName,
                headerData.getBytes(StandardCharsets.UTF_8));
            record.headers().add(headerName, sv);
        } catch (IOException e) {
            throw new WasmFunctionException(functionName, e);
        }
    }

    //
    // Key
    //

    private void getKeyFn(ExtismCurrentPlugin plugin, LibExtism.ExtismVal[] params, LibExtism.ExtismVal[] returns,
        Optional<HostUserData> userData) {
        final R record = this.ref.get();
        final byte[] rawData = recordConverter.fromConnectKey(record);
        plugin.returnBytes(returns[0], rawData);
    }

    private void setKeyFn(ExtismCurrentPlugin plugin, LibExtism.ExtismVal[] args, LibExtism.ExtismVal[] returns,
        Optional<HostUserData> userData) {
        final R record = this.ref.get();
        final SchemaAndValue sv = recordConverter.toConnectKey(record, plugin.inputBytes(args[0]));

        this.ref.set(
            record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                sv.schema(),
                sv.value(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                record.headers()));
    }

    //
    // Value
    //

    private void getValueFn(ExtismCurrentPlugin plugin, LibExtism.ExtismVal[] params, LibExtism.ExtismVal[] returns,
        Optional<HostUserData> userData) {
        final R record = this.ref.get();
        final byte[] rawData = recordConverter.fromConnectValue(record);

        plugin.returnBytes(returns[0], rawData);
    }

    private void setValueFn(ExtismCurrentPlugin plugin, LibExtism.ExtismVal[] args, LibExtism.ExtismVal[] returns,
        Optional<HostUserData> userData) {
        final R record = this.ref.get();
        final SchemaAndValue sv = recordConverter.toConnectValue(record, plugin.inputBytes(args[0]));

        this.ref.set(
            record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                sv.schema(),
                sv.value(),
                record.timestamp(),
                record.headers()));
    }

    //
    // Topic
    //

    private void getTopicFn(ExtismCurrentPlugin plugin, LibExtism.ExtismVal[] args, LibExtism.ExtismVal[] returns,
        Optional<HostUserData> userData) {
        final R record = this.ref.get();
        byte[] rawData = record.topic().getBytes(StandardCharsets.UTF_8);

        plugin.returnBytes(returns[0], rawData);
    }

    private void setTopicFn(ExtismCurrentPlugin plugin, LibExtism.ExtismVal[] args, LibExtism.ExtismVal[] returns,
        Optional<HostUserData> userData) {
        final R record = this.ref.get();

        this.ref.set(
            record.newRecord(
                plugin.inputString(args[0]),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                record.headers()));
    }

    //    //
    //    // Record
    //    //
    //
    //    private Value[] getRecordFn(Instance instance, Value... args) {
    //        final R record = this.ref.get();
    //
    //        WasmRecord env = new WasmRecord();
    //        env.topic = record.topic();
    //        env.key = recordConverter.fromConnectKey(record);
    //        env.value = recordConverter.fromConnectValue(record);
    //
    //        if (record.headers() != null) {
    //            // May not be needed but looks like the record headers may be required
    //            // by key/val converters
    //            for (Header header : record.headers()) {
    //                env.headers.put(header.key(), recordConverter.fromConnectHeader(record, header));
    //            }
    //        }
    //
    //        try {
    //            byte[] rawData = MAPPER.writeValueAsBytes(env);
    //
    //            return new Value[] {
    //                    write(rawData)
    //            };
    //        } catch (Exception e) {
    //            throw new RuntimeException(e);
    //        }
    //    }
    //
    //    private Value[] setRecordFn(Instance instance, Value... args) {
    //        final int addr = args[0].asInt();
    //        final int size = args[1].asInt();
    //        final R record = this.ref.get();
    //        final byte[] in = instance.memory().readBytes(addr, size);
    //
    //        try {
    //            WasmRecord w = MAPPER.readValue(in, WasmRecord.class);
    //
    //            // May not be needed but looks like the record headers may be required
    //            // by key/val converters so let's do it even if I don't think the way
    //            // I'm doing it is 100% correct :)
    //
    //            Headers connectHeaders = new ConnectHeaders();
    //
    //            w.headers.forEach((k, v) -> {
    //                connectHeaders.add(k, recordConverter.toConnectHeader(record, k, v));
    //            });
    //
    //            SchemaAndValue keyAndSchema = recordConverter.toConnectKey(record, w.key);
    //            SchemaAndValue valueAndSchema = recordConverter.toConnectValue(record, w.value);
    //
    //            this.ref.set(
    //                record.newRecord(
    //                    w.topic,
    //                    record.kafkaPartition(),
    //                    keyAndSchema.schema(),
    //                    keyAndSchema.value(),
    //                    valueAndSchema.schema(),
    //                    valueAndSchema.value(),
    //                    record.timestamp(),
    //                    connectHeaders));
    //        } catch (Exception e) {
    //            throw new RuntimeException(e);
    //        }
    //
    //        return new Value[] {};
    //    }
}
