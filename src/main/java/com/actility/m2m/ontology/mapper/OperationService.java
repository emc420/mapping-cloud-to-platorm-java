package com.actility.m2m.ontology.mapper;

import com.actility.m2m.flow.data.DownMessage;
import com.actility.m2m.flow.data.UpMessage;
import com.actility.m2m.ontology.mapping.java.lib.data.DownOperation;
import com.actility.m2m.ontology.mapping.java.lib.data.UpOperation;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

public class OperationService {

    private final OperationFactory operationFactory;

    @Inject
    public OperationService(OperationFactory operationFactory) {
        this.operationFactory = operationFactory;
    }
    @Nonnull
    public Optional<UpMessage> applyUpOperations(@Nonnull UpMessage message, @Nonnull List<UpOperation> operations) {
        Optional<UpMessage> messageOptional = Optional.of(message);
        for (UpOperation operation : operations) {
            if(!messageOptional.isPresent()){
                return Optional.empty();
            }
            messageOptional = this.operationFactory.build(operation).applyUpOperation(messageOptional.get(), operation);
        }
        return messageOptional;
    }
    @Nonnull
    public Optional<DownMessage> applyDownOperations(@Nonnull DownMessage message, @Nonnull List<DownOperation> operations) {
        Optional<DownMessage> messageOptional = Optional.of(message);
        for (DownOperation operation : operations) {
            if(!messageOptional.isPresent()){
                return Optional.empty();
            }
            messageOptional = this.operationFactory.build(operation).applyDownOperation(messageOptional.get(), operation);
        }
        return messageOptional;
    }
}