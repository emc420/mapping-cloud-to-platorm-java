package com.actility.m2m.ontology.mapper.operations;

import com.actility.m2m.commons.service.mapper.JsonMapper;
import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.actility.m2m.flow.data.*;
import com.actility.m2m.ontology.mapper.OperationHandler;
import com.actility.m2m.ontology.mapper.jmespath.*;
import com.actility.m2m.ontology.mapping.java.lib.data.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javax.annotation.Nonnull;
import java.util.*;

public class DownExtractDriverMessageOperation implements OperationHandler {

    private static final JsonMapper jsonMapper = new JsonMapper(ObjectMapperModule.createObjectMapper());

    @Override
    public Optional<UpMessage> applyUpOperation(@Nonnull UpMessage message, @Nonnull UpOperation upOperation) {
        return Optional.empty();
    }
    @Override
    @Nonnull
    public Optional<DownMessage> applyDownOperation(@Nonnull DownMessage message, @Nonnull DownOperation downOperation) {
        JsonNode messageJson = jsonMapper.toJsonNode(message);
        Command command = message.command;
        JsonNode resultJson = null;
        DownExtractDriverMessage jmesPathOperation = (DownExtractDriverMessage) downOperation;
        for (Map.Entry<String, JsonNode> entry : jmesPathOperation.commands.entrySet()){
            if(command!=null && entry.getKey().contains(command.id)){
                resultJson = JmesPathUtil.extractMessage(messageJson, entry.getValue());
                break;
            }
        }
        if(resultJson==null){
            if(jmesPathOperation.commands.containsKey("default")){
                resultJson = JmesPathUtil.extractMessage(messageJson, jmesPathOperation.commands.get("default"));
            }
        }
        return Optional.of(DownMessage.newDownMessageBuilder(message)
                .packet(MessagePacket.newMessagePacketBuilder()
                        .message(jsonMapper.fromJson(resultJson, ObjectNode.class))
                        .build())
                .build());
    }
}