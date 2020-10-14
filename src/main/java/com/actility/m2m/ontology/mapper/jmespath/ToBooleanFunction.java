package com.actility.m2m.ontology.mapper.jmespath;

import io.burt.jmespath.Adapter;
import io.burt.jmespath.JmesPathType;
import io.burt.jmespath.function.ArgumentConstraints;
import io.burt.jmespath.function.BaseFunction;
import io.burt.jmespath.function.FunctionArgument;

import java.util.List;

public class ToBooleanFunction extends BaseFunction {

    public ToBooleanFunction() {
        super(ArgumentConstraints.typeOf(JmesPathType.NUMBER));
    }

    @Override
    protected <T> T callFunction(Adapter<T> runtime, List<FunctionArgument<T>> arguments) {
        boolean result = false;
        T value = arguments.get(0).value();
        if ((int) runtime.toNumber(value) == 1) {
            result = true;
        }

        return runtime.createBoolean(result);
    }
}
