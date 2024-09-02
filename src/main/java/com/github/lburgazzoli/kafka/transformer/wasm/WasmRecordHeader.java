package com.github.lburgazzoli.kafka.transformer.wasm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class WasmRecordHeader {
    @JsonProperty
    String key;

    @JsonProperty
    byte[] value;

    public WasmRecordHeader(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }
}
