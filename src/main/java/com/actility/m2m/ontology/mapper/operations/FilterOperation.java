package com.actility.m2m.ontology.mapper.operations;

import com.actility.m2m.commons.service.mapper.JsonMapper;
import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.actility.m2m.flow.data.DownMessage;
import com.actility.m2m.flow.data.UpMessage;
import com.actility.m2m.ontology.mapper.OperationHandler;
import com.actility.m2m.ontology.mapping.java.lib.data.DownOperation;
import com.actility.m2m.ontology.mapping.java.lib.data.UpFilterOperation;
import com.actility.m2m.ontology.mapping.java.lib.data.UpOperation;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

public class FilterOperation implements OperationHandler {

    private static final JsonMapper jsonMapper = new JsonMapper(ObjectMapperModule.createObjectMapper());

    @Override
    public Optional<UpMessage> applyUpOperation(@Nonnull UpMessage message, @Nonnull UpOperation upOperation) {

        UpFilterOperation upFilterOperation = (UpFilterOperation) upOperation;
        if (isFilterPresent(upFilterOperation.keepDeviceDownlinkSent, message.type.getValue(), "deviceDownlinkSent")) {
            return Optional.of(message);
        }
        if (isFilterPresent(upFilterOperation.keepDeviceUplink, message.type.getValue(), "deviceUplink")) {
            return Optional.of(message);
        }
        if (isFilterPresent(upFilterOperation.keepDeviceLocation, message.type.getValue(), "deviceLocation")) {
            return Optional.of(message);
        }
        if (isFilterPresent(upFilterOperation.keepDeviceNotification, message.type.getValue(), "deviceNotification")
                && isSubtypePresent(upFilterOperation.keepDeviceNotificationSubTypes, message.subType)) {
            return Optional.of(message);
        }

        return Optional.empty();
    }

    public boolean isFilterPresent(Boolean filterVal, String type, String typeString) {
        return filterVal != null && filterVal && type.equalsIgnoreCase(typeString);
    }

    public boolean isSubtypePresent(List<String> subTypes, String subType) {
        if (subTypes == null) {
            return true;
        }
        if (subTypes.isEmpty() && subType == null) {
            return true;
        }
        if (subType != null && subTypes.contains(subType)) {
            return true;
        }
        return false;
    }
    @Override
    public Optional<DownMessage> applyDownOperation(DownMessage message, DownOperation upOperation) {
        return Optional.empty();
    }
}