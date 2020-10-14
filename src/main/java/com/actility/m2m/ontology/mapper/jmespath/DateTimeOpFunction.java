package com.actility.m2m.ontology.mapper.jmespath;

import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.burt.jmespath.Adapter;
import io.burt.jmespath.JmesPathType;
import io.burt.jmespath.function.ArgumentConstraints;
import io.burt.jmespath.function.BaseFunction;
import io.burt.jmespath.function.FunctionArgument;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class DateTimeOpFunction extends BaseFunction {
    private ObjectMapper mapper;

    public DateTimeOpFunction() {
        super(
                ArgumentConstraints.listOf(
                        ArgumentConstraints.typeOf(JmesPathType.STRING),
                        ArgumentConstraints.typeOf(JmesPathType.STRING),
                        ArgumentConstraints.typeOf(JmesPathType.NUMBER, JmesPathType.STRING),
                        ArgumentConstraints.typeOf(JmesPathType.STRING)));
        mapper = ObjectMapperModule.createObjectMapper();
    }

    @Override
    protected <T> T callFunction(Adapter<T> runtime, List<FunctionArgument<T>> arguments) {
        String time = mapper.convertValue(arguments.get(0).value(), String.class);
        String operation = mapper.convertValue(arguments.get(1).value(), String.class);
        int age = Integer.parseInt(mapper.convertValue(arguments.get(2).value(), String.class));
        String unit = mapper.convertValue(arguments.get(3).value(), String.class);
        Operator operator = Operator.fromValue(operation);
        switch (operator) {
            case ADD:
                return runtime.createString(add(time, age, unit));
            case SUB:
                return runtime.createString(subtract(time, age, unit));
            default:
                return runtime.createString("Invalid Operator");
        }
    }

    public String subtract(String time, int age, String unit) {
        OffsetDateTime currentTime = OffsetDateTime.parse(time);
        switch (unit) {
            case "s":
                return currentTime.minus(age, ChronoUnit.SECONDS).toString();
            case "ms":
                return currentTime.minus(age, ChronoUnit.MILLIS).toString();
            case "m":
                return currentTime.minus(age, ChronoUnit.MINUTES).toString();
        }
        return time;
    }

    public String add(String time, int age, String unit) {

        OffsetDateTime currentTime = OffsetDateTime.parse(time);
        switch (unit) {
            case "s":
                return currentTime.plus(age, ChronoUnit.SECONDS).toString();
            case "ms":
                return currentTime.plus(age, ChronoUnit.MILLIS).toString();
            case "m":
                return currentTime.plus(age, ChronoUnit.MINUTES).toString();
        }
        return time;
    }
}
