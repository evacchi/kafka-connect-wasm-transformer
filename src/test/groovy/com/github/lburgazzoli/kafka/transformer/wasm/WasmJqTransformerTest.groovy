package com.github.lburgazzoli.kafka.transformer.wasm

import com.github.lburgazzoli.kafka.support.EmbeddedKafkaConnect
import com.github.lburgazzoli.kafka.support.EmbeddedKafkaContainer
import com.github.lburgazzoli.kafka.transformer.wasm.support.WasmTransformerTestSpec
import groovy.util.logging.Slf4j
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.file.FileStreamSourceConnector
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.runtime.isolation.PluginDiscoveryMode
import org.apache.kafka.connect.storage.StringConverter
import org.testcontainers.spock.Testcontainers
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.TempDir

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Duration

@Slf4j
@Testcontainers
class WasmJqTransformerTest extends WasmTransformerTestSpec {

    public static final String WASM_JQ_FILTER = "jq.filter";

    @Shared
    EmbeddedKafkaContainer KAFKA = new EmbeddedKafkaContainer()

    @TempDir
    Path connectTmp

    def 'direct transformer (jq)'() {
        given:
            def t = new WasmTransformer()
            t.configure(Map.of(
                    WasmTransformer.WASM_MODULE_PATH, 'src/test/resources/plugin-jq.wasm',
                    WasmTransformer.WASM_FUNCTION_NAME, 'transform',
                    WASM_JQ_FILTER, 'del(.email)' // deletes the email field from the incoming record
            ))

            def recordIn = sourceRecord()
                    .withTopic('foo')
                    .withKey('the-key'.getBytes(StandardCharsets.UTF_8))
                    .withValue('{"user-id": "evacchi", "email": "edoardo@dylibso.com"}'.getBytes(StandardCharsets.UTF_8))
                    .build()

        when:
            def recordOut = t.apply(recordIn)
        then:
            recordOut.value() == '{"user-id":"evacchi"}'.getBytes(StandardCharsets.UTF_8)
        cleanup:
            closeQuietly(t)
    }

    def 'direct transformer with fuel (jq)'() {
        given:
        def t = new WasmTransformer()
        t.configure(Map.of(
                WasmTransformer.WASM_MODULE_PATH, 'src/test/resources/plugin-jq.wasm',
                WasmTransformer.WASM_FUNCTION_NAME, 'transform',
                WASM_JQ_FILTER, 'del(.email)', // deletes the email field from the incoming record
                WasmTransformer.WASM_FUEL_LIMIT, 7_500_000,
        ))

        def recordIn = sourceRecord()
                .withTopic('foo')
                .withKey('the-key'.getBytes(StandardCharsets.UTF_8))
                .withValue('{"user-id": "evacchi", "email": "edoardo@dylibso.com"}'.getBytes(StandardCharsets.UTF_8))
                .build()

        when:
        def recordOut = t.apply(recordIn)
        then:
        recordOut.value() == '{"user-id":"evacchi"}'.getBytes(StandardCharsets.UTF_8)
        cleanup:
        closeQuietly(t)
    }

    def 'direct transformer with fuel below threshold (jq)'() {
        given:
        def t = new WasmTransformer()
        t.configure(Map.of(
                WasmTransformer.WASM_MODULE_PATH, 'src/test/resources/plugin-jq.wasm',
                WasmTransformer.WASM_FUNCTION_NAME, 'transform',
                WASM_JQ_FILTER, 'del(.email)', // deletes the email field from the incoming record
                WasmTransformer.WASM_FUEL_LIMIT, 10,
        ))

        def recordIn = sourceRecord()
                .withTopic('foo')
                .withKey('the-key'.getBytes(StandardCharsets.UTF_8))
                .withValue('{"user-id": "evacchi", "email": "edoardo@dylibso.com"}'.getBytes(StandardCharsets.UTF_8))
                .build()

        when:
            t.apply(recordIn)
        then:
            thrown WasmTransformerException
        cleanup:
            closeQuietly(t)
    }


    def 'pipeline transformer (jq)'() {

        given:
            def inFile = connectTmp.resolve('in.txt')
            def topic = UUID.randomUUID().toString()
            def content  = '{"user-id": "evacchi", "email": "edoardo@dylibso.com"}'

            Producer<byte[], byte[]> producer = KAFKA.producer()
            Consumer<byte[], byte[]> consumer = KAFKA.consumer()

            def kc = new EmbeddedKafkaConnect()
            kc.setProperty(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers)
            kc.setProperty(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.name)
            kc.setProperty(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.name)
            kc.setProperty(WorkerConfig.PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.SERVICE_LOAD.name())

            kc.setConnectorDefinition('file-source', FileStreamSourceConnector.class, Map.of(
                    FileStreamSourceConnector.FILE_CONFIG, inFile.toString(),
                    FileStreamSourceConnector.TOPIC_CONFIG, topic,
                    ConnectorConfig.TRANSFORMS_CONFIG, 'wasm',
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.type', WasmTransformer.class.name,
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.WASM_MODULE_PATH, 'src/test/resources/plugin-jq.wasm',
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.WASM_FUNCTION_NAME, 'transform',
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.KEY_CONVERTER, StringConverter.class.name,
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.VALUE_CONVERTER, StringConverter.class.name,
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.HEADER_CONVERTER, StringConverter.class.name,
                    ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WASM_JQ_FILTER, 'del(.email)' // deletes the email field from the incoming record,
            ))

            kc.start()

            // subscribe to the topic
            consumer.subscribe(Collections.singletonList(topic))

        when:
            // write something in the input file
            Files.writeString(inFile, content + '\n', StandardOpenOption.APPEND, StandardOpenOption.CREATE)

        then:
            def records = consumer.poll(Duration.ofSeconds(5))
            records.size() == 1

            with(records.iterator().next()) {
                it.value() == '{"user-id":"evacchi"}'.getBytes(StandardCharsets.UTF_8)
            }

        cleanup:
            closeQuietly(producer)
            closeQuietly(consumer)
            closeQuietly(kc)
    }

    def 'pipeline transformer with fuel (jq)'() {

        given:
        def inFile = connectTmp.resolve('in.txt')
        def topic = UUID.randomUUID().toString()
        def content  = '{"user-id": "evacchi", "email": "edoardo@dylibso.com"}'

        Producer<byte[], byte[]> producer = KAFKA.producer()
        Consumer<byte[], byte[]> consumer = KAFKA.consumer()

        def kc = new EmbeddedKafkaConnect()
        kc.setProperty(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers)
        kc.setProperty(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.name)
        kc.setProperty(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.name)
        kc.setProperty(WorkerConfig.PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.SERVICE_LOAD.name())

        kc.setConnectorDefinition('file-source', FileStreamSourceConnector.class, Map.ofEntries(
                Map.entry(FileStreamSourceConnector.FILE_CONFIG, inFile.toString()),
                Map.entry(FileStreamSourceConnector.TOPIC_CONFIG, topic),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG, 'wasm'),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.type', WasmTransformer.class.name),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.WASM_MODULE_PATH, 'src/test/resources/plugin-jq.wasm'),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.WASM_FUNCTION_NAME, 'transform'),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.KEY_CONVERTER, StringConverter.class.name),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.VALUE_CONVERTER, StringConverter.class.name),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.HEADER_CONVERTER, StringConverter.class.name),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WASM_JQ_FILTER, 'del(.email)'), // deletes the email field from the incoming record
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.WASM_FUEL_LIMIT, Long.toString(7_500_000))
        ))

        kc.start()

        // subscribe to the topic
        consumer.subscribe(Collections.singletonList(topic))

        when:
        // write something in the input file
        Files.writeString(inFile, content + '\n', StandardOpenOption.APPEND, StandardOpenOption.CREATE)

        then:
        def records = consumer.poll(Duration.ofSeconds(5))
        records.size() == 1

        with(records.iterator().next()) {
            it.value() == '{"user-id":"evacchi"}'.getBytes(StandardCharsets.UTF_8)
        }

        cleanup:
        closeQuietly(producer)
        closeQuietly(consumer)
        closeQuietly(kc)
    }



    def 'pipeline transformer with fuel below threshold (jq)'() {

        given:
        def inFile = connectTmp.resolve('in.txt')
        def topic = UUID.randomUUID().toString()
        def content  = '{"user-id": "evacchi", "email": "edoardo@dylibso.com"}'

        Producer<byte[], byte[]> producer = KAFKA.producer()
        Consumer<byte[], byte[]> consumer = KAFKA.consumer()

        def kc = new EmbeddedKafkaConnect()
        kc.setProperty(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers)
        kc.setProperty(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.name)
        kc.setProperty(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.name)
        kc.setProperty(WorkerConfig.PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.SERVICE_LOAD.name())

        kc.setConnectorDefinition('file-source', FileStreamSourceConnector.class, Map.ofEntries(
                Map.entry(FileStreamSourceConnector.FILE_CONFIG, inFile.toString()),
                Map.entry(FileStreamSourceConnector.TOPIC_CONFIG, topic),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG, 'wasm'),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.type', WasmTransformer.class.name),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.WASM_MODULE_PATH, 'src/test/resources/plugin-jq.wasm'),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.WASM_FUNCTION_NAME, 'transform'),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.KEY_CONVERTER, StringConverter.class.name),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.VALUE_CONVERTER, StringConverter.class.name),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.HEADER_CONVERTER, StringConverter.class.name),
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WASM_JQ_FILTER, 'del(.email)'), // deletes the email field from the incoming record
                Map.entry(ConnectorConfig.TRANSFORMS_CONFIG + '.wasm.' + WasmTransformer.WASM_FUEL_LIMIT, Long.toString(10))
        ))

        kc.start()

        // subscribe to the topic
        consumer.subscribe(Collections.singletonList(topic))

        when:
        // write something in the input file
        Files.writeString(inFile, content + '\n', StandardOpenOption.APPEND, StandardOpenOption.CREATE)

        then:
        def records = consumer.poll(Duration.ofSeconds(5))
        records.size() == 0

        cleanup:
        closeQuietly(producer)
        closeQuietly(consumer)
        closeQuietly(kc)
    }
}
