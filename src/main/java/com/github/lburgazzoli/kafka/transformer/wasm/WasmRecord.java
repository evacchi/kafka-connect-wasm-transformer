package com.github.lburgazzoli.kafka.transformer.wasm;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class WasmRecord {
    @JsonProperty
    public List<WasmRecordHeader> headers = new ArrayList<>();

    @JsonProperty
    public String topic;

    @JsonProperty
    public byte[] key;

    @JsonProperty
    public byte[] value;
}
