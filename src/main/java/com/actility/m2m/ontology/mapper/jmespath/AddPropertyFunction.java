package com.actility.m2m.ontology.mapper.jmespath;

import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.burt.jmespath.Adapter;
import io.burt.jmespath.JmesPathType;
import io.burt.jmespath.function.ArgumentConstraints;
import io.burt.jmespath.function.FunctionArgument;
import io.burt.jmespath.function.BaseFunction;

import java.util.List;

public class AddPropertyFunction extends BaseFunction{
    private ObjectMapper mapper;
    public AddPropertyFunction() {
        super(
                ArgumentConstraints.listOf(
                        ArgumentConstraints.typeOf(JmesPathType.OBJECT),
                        ArgumentConstraints.typeOf(JmesPathType.STRING),
                        ArgumentConstraints.typeOf(JmesPathType.STRING, JmesPathType.OBJECT)));
        mapper = ObjectMapperModule.createObjectMapper();
    }

    @Override
    protected <T> T callFunction(Adapter<T> runtime, List<FunctionArgument<T>> arguments) {

        ObjectNode obj = (ObjectNode) arguments.get(0).value();
        obj.set(mapper.convertValue(arguments.get(1).value(), String.class), mapper.convertValue(arguments.get(2).value(), JsonNode.class));
        return (T) obj;
    }


}
