package com.actility.m2m.ontology.mapper;

import com.actility.m2m.flow.data.DownMessage;
import com.actility.m2m.flow.data.UpMessage;
import com.actility.m2m.ontology.mapping.java.lib.data.DownOperation;
import com.actility.m2m.ontology.mapping.java.lib.data.UpOperation;

import java.util.Optional;

public interface OperationHandler {
    Optional<UpMessage> applyUpOperation(UpMessage message, UpOperation upOperation);
    Optional<DownMessage> applyDownOperation(DownMessage message, DownOperation downOperation);
}