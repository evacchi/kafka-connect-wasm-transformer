package com.github.lburgazzoli.kafka.transformer.wasm;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class WasmRecord {
    @JsonProperty
    public Map<String, byte[]> headers = new HashMap<>();

    @JsonProperty
    public String topic;

    @JsonProperty
    public byte[] key;

    @JsonProperty
    public byte[] value;
}
