package com.github.lburgazzoli.kafka.transformer.wasm;

public class WasmTransformerException extends RuntimeException {
    private String functionName;

    public WasmTransformerException(String functionName, String message) {
        super(message);

        this.functionName = functionName;
    }

    public WasmTransformerException(String functionName, String message, Throwable cause) {
        super(message, cause);

        this.functionName = functionName;
    }

    public WasmTransformerException(String functionName, Throwable cause) {
        super(cause);

        this.functionName = functionName;
    }

    public String getFunctionName() {
        return functionName;
    }
}
