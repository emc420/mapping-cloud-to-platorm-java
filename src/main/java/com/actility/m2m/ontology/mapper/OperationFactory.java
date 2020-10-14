package com.actility.m2m.ontology.mapper;

import com.actility.m2m.ontology.mapper.operations.*;
import com.actility.m2m.ontology.mapping.java.lib.data.*;

import javax.inject.Inject;

public class OperationFactory {

    @Inject
    public OperationFactory() {
    }

    public OperationHandler build(UpOperation operation) {
        if (operation.getClass() == UpExtractPoints.class) {
            return new UpExtractPointsOperation();
        }else if(operation.getClass() == UpFilterOperation.class){
            return new FilterOperation();
        }else if(operation.getClass() == UpFilterPointsOperation.class){
            return new FilterPointsOperation();
        }else if(operation.getClass() == UpUpdatePoints.class){
            return new UpUpdatePointsOperation();
        } else {
            throw new UnsupportedOperationException("Unknown Operation " + operation.getClass());
        }
    }
    public OperationHandler build(DownOperation operation) {
        if (operation.getClass() == DownExtractDriverMessage.class) {
            return new DownExtractDriverMessageOperation();
        }else if(operation.getClass() == DownUpdateCommand.class){
            return new DownUpdateCommandOperation();
        } else {
            throw new UnsupportedOperationException("Unknown Operation " + operation.getClass());
        }
    }
}