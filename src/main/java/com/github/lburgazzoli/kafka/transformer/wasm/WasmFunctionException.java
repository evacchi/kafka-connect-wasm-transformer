package com.github.lburgazzoli.kafka.transformer.wasm;

public class WasmFunctionException extends RuntimeException {
    private String functionName;

    public WasmFunctionException(String functionName, String message) {
        super(message);

        this.functionName = functionName;
    }

    public WasmFunctionException(String functionName, String message, Throwable cause) {
        super(message, cause);

        this.functionName = functionName;
    }

    public WasmFunctionException(String functionName, Throwable cause) {
        super(cause);

        this.functionName = functionName;
    }

    public String getFunctionName() {
        return functionName;
    }
}
