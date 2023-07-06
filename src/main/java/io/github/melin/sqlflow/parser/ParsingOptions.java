package io.github.melin.sqlflow.parser;

import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/21 1:28 PM
 */
public class ParsingOptions {
    public enum DecimalLiteralTreatment {
        AS_DOUBLE,
        AS_DECIMAL,
        REJECT
    }

    private final DecimalLiteralTreatment decimalLiteralTreatment;

    public ParsingOptions() {
        this(DecimalLiteralTreatment.REJECT);
    }

    public ParsingOptions(DecimalLiteralTreatment decimalLiteralTreatment) {
        this.decimalLiteralTreatment = requireNonNull(decimalLiteralTreatment, "decimalLiteralTreatment is null");
    }

    public DecimalLiteralTreatment getDecimalLiteralTreatment() {
        return decimalLiteralTreatment;
    }
}
