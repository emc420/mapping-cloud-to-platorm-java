package com.actility.m2m.ontology.mapper.operations;

import com.actility.m2m.commons.service.mapper.JsonMapper;
import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.actility.m2m.flow.data.*;
import com.actility.m2m.ontology.mapper.OperationHandler;
import com.actility.m2m.ontology.mapper.jmespath.*;
import com.actility.m2m.ontology.mapping.java.lib.data.*;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.*;

public class DownUpdateCommandOperation implements OperationHandler {

    private static final JsonMapper jsonMapper = new JsonMapper(ObjectMapperModule.createObjectMapper());

    @Override
    public Optional<UpMessage> applyUpOperation(UpMessage message, UpOperation upOperation) {
        return Optional.empty();
    }
    @Override
    public Optional<DownMessage> applyDownOperation(DownMessage message, DownOperation downOperation) {
        Command command = message.command;
        String id=null;
        JsonNode input=null;
        DownUpdateCommand jmesPathOperation = (DownUpdateCommand) downOperation;
        for (Map.Entry<String, UpdateCommand> entry : jmesPathOperation.commands.entrySet()){
            if(command == null){
                break;
            }
            if(command.id.equalsIgnoreCase(entry.getKey())){
                if(entry.getValue().id!=null){
                    id = entry.getValue().id;
                }
                if(entry.getValue().input!=null){
                    input = JmesPathUtil.extractCommands(jsonMapper.toJsonNode(command),entry.getValue().input);
                }
                return Optional.of(DownMessage.newDownMessageBuilder(message).
                        command(Command.newCommandBuilder().id(id!=null? id : command.id).input(input!=null?input:command.input).build()).build());
            }
        }
        if(command!=null && jmesPathOperation.commands.containsKey("default")){
            id = jmesPathOperation.commands.get("default").id;

            if(jmesPathOperation.commands.get("default").input!=null){
                input = JmesPathUtil.extractCommands(jsonMapper.toJsonNode(command), jmesPathOperation.commands.get("default").input);
            }
            return Optional.of(DownMessage.newDownMessageBuilder(message).
                    command(Command.newCommandBuilder().id(id!=null? id : command.id).input(input!=null?input:command.input).build()).build());
        }
        return Optional.of(DownMessage.newDownMessageBuilder(message).
                    command(command).build());
    }
}
