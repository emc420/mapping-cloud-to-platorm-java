package com.actility.m2m.ontology.mapper.jmespath;

import java.util.NoSuchElementException;
import java.util.Objects;

import static java.lang.String.format;

public enum Operator {
    ADD("+"),
    SUB("-");

    public final String value;

    private Operator(String value) {
        this.value = value;
    }

    public static Operator fromValue(String value) {

        for (Operator operator : values()) {
            if (Objects.equals(operator.value, value)) {
                return operator;
            }
        }

        throw new NoSuchElementException(format("unknown operator: '%s'", value));
    }
}
