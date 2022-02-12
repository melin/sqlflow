package com.github.melin.sqlflow;

import com.github.melin.sqlflow.tree.NodeLocation;

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/22 11:43 AM
 */
public class SqlFlowException extends RuntimeException {

    private final Optional<NodeLocation> location;

    public SqlFlowException(String message) {
        this(Optional.empty(), message, null);
    }

    public SqlFlowException(String message, Throwable cause) {
        this(Optional.empty(), message, cause);
    }

    public SqlFlowException(Optional<NodeLocation> location, String message, Throwable cause) {
        super(message, cause);
        this.location = requireNonNull(location, "location is null");
    }

    public Optional<NodeLocation> getLocation() {
        return location;
    }

    @Override
    public String getMessage() {
        String message = getRawMessage();
        if (location.isPresent()) {
            message = format("line %s:%s: %s", location.get().getLineNumber(), location.get().getColumnNumber(), message);
        }
        return message;
    }

    public String getRawMessage() {
        String message = super.getMessage();
        if (message == null && getCause() != null) {
            message = getCause().getMessage();
        }
        return message;
    }
}
