package com.actility.m2m.ontology.mapper.operations;

import com.actility.m2m.commons.service.mapper.JsonMapper;
import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.actility.m2m.flow.data.*;
import com.actility.m2m.ontology.mapper.MessageExtractionException;
import com.actility.m2m.ontology.mapper.PointExtractionException;
import com.actility.m2m.ontology.mapping.java.lib.data.*;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DownUpdateCommandOperationTest {

    private static final JsonMapper jsonMapper = new JsonMapper(ObjectMapperModule.createObjectMapper());
    DownUpdateCommandOperation jmesPathOperation = new DownUpdateCommandOperation();

    private DownMessage buildInputDownMessage(String inputMessageFile) throws IOException {
        ObjectNode message =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream(inputMessageFile));
        return DownMessage.newDownMessageBuilder()
                .id("00000000-000000-00000-000000000")
                .time(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                .subAccount(Account.newAccountBuilder().id("subAccount1").realmId("subRealm1").build())
                .origin(
                        DownOrigin.newDownOriginBuilder().id("tpw").type(DownOriginType.PROCESSOR).time(OffsetDateTime.now()).build())
                .content(JsonNodeFactory.instance.objectNode())
                .type(DownMessageType.DEVICEDOWNLINK)
                .command(Command.newCommandBuilder().id(message.get("id").asText()).input(message.get("input")).build())
                .thing(Thing.newThingBuilder().key("lora:0102030405060708").build())
                .subscriber(
                        Subscriber.newSubscriberBuilder().id("sub1").realmId("realm1").build())
                .build();
    }

    @Test
    public void should_update_command_id_when_there_is_a_match() throws PointExtractionException, IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("update_command_id.json");
        Map<String, UpdateCommand> updateCommands = new HashMap<>();
        updateCommands.put("myDeviceCommand", UpdateCommand.newUpdateCommandBuilder().id("newCommandId").build());
        DownUpdateCommand jmespath = DownUpdateCommand.newDownUpdateCommandBuilder().commands(updateCommands).build();
        //When
        Optional<DownMessage> outputMessage = jmesPathOperation.applyDownOperation(inputDownMessage, jmespath);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("update_command_id_expected.json"));
        Optional<DownMessage> expectedOutputMessage = Optional.of(DownMessage.newDownMessageBuilder(inputDownMessage).command(Command.newCommandBuilder().id(expectedMessage.get("id").asText()).input(expectedMessage.get("input")).build()).build());
        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }
    @Test
    public void should_update_command_input_when_there_is_a_match() throws PointExtractionException, IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("update_command_id.json");
        ObjectNode inputMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("update_command_input_jmespath.json"));
        Map<String, UpdateCommand> updateCommands = new HashMap<>();
        updateCommands.put("myDeviceCommand", UpdateCommand.newUpdateCommandBuilder().input(inputMessage).build());
        DownUpdateCommand jmespath = DownUpdateCommand.newDownUpdateCommandBuilder().commands(updateCommands).build();
        //When
        Optional<DownMessage> outputMessage = jmesPathOperation.applyDownOperation(inputDownMessage, jmespath);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("update_command_input_jmespath_expected.json"));
        Optional<DownMessage> expectedOutputMessage = Optional.of(DownMessage.newDownMessageBuilder(inputDownMessage).command(Command.newCommandBuilder().id(expectedMessage.get("id").asText()).input(expectedMessage.get("input")).build()).build());
        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }
    @Test
    public void should_update_command_id_input_when_there_is_a_match() throws PointExtractionException, IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("update_command_id.json");
        ObjectNode inputMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("update_command_id_input_jmespath.json"));
        Map<String, UpdateCommand> updateCommands = new HashMap<>();
        updateCommands.put("myDeviceCommand", UpdateCommand.newUpdateCommandBuilder().id(inputMessage.get("id").asText()).input(inputMessage.get("input")).build());
        DownUpdateCommand jmespath = DownUpdateCommand.newDownUpdateCommandBuilder().commands(updateCommands).build();
        //When
        Optional<DownMessage> outputMessage = jmesPathOperation.applyDownOperation(inputDownMessage, jmespath);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("update_command_id_input_jmespath_expected.json"));
        Optional<DownMessage> expectedOutputMessage = Optional.of(DownMessage.newDownMessageBuilder(inputDownMessage).command(Command.newCommandBuilder().id(expectedMessage.get("id").asText()).input(expectedMessage.get("input")).build()).build());
        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }
    @Test
    public void should_do_nothing_command_id_is_a_mismatch() throws PointExtractionException, IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("update_command_id.json");
        ObjectNode inputMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("update_command_id_input_jmespath.json"));
        Map<String, UpdateCommand> updateCommands = new HashMap<>();
        updateCommands.put("misMatchCommand", UpdateCommand.newUpdateCommandBuilder().id(inputMessage.get("id").asText()).input(inputMessage.get("input")).build());
        DownUpdateCommand jmespath = DownUpdateCommand.newDownUpdateCommandBuilder().commands(updateCommands).build();
        //When
        Optional<DownMessage> outputMessage = jmesPathOperation.applyDownOperation(inputDownMessage, jmespath);
        //Then
        Optional<DownMessage> expectedOutputMessage = Optional.of(DownMessage.newDownMessageBuilder(inputDownMessage).build());
        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }
    @Test
    public void should_update_command_id_when_there_is_a_default_mismatch() throws PointExtractionException, IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("update_command_id.json");
        Map<String, UpdateCommand> updateCommands = new HashMap<>();
        updateCommands.put("default", UpdateCommand.newUpdateCommandBuilder().id("default").build());
        DownUpdateCommand jmespath = DownUpdateCommand.newDownUpdateCommandBuilder().commands(updateCommands).build();
        //When
        Optional<DownMessage> outputMessage = jmesPathOperation.applyDownOperation(inputDownMessage, jmespath);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("update_command_id_default_expected.json"));
        Optional<DownMessage> expectedOutputMessage = Optional.of(DownMessage.newDownMessageBuilder(inputDownMessage).command(Command.newCommandBuilder().id(expectedMessage.get("id").asText()).input(expectedMessage.get("input")).build()).build());
        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }

    @Test
    public void should_update_command_input_when_there_is_a_default_mismatch() throws PointExtractionException, IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("update_command_id.json");
        Map<String, UpdateCommand> updateCommands = new HashMap<>();
        updateCommands.put("default", UpdateCommand.newUpdateCommandBuilder().input(jsonMapper.toJsonNode("\"{{ @.input.prop1}}\"")).build());
        DownUpdateCommand jmespath = DownUpdateCommand.newDownUpdateCommandBuilder().commands(updateCommands).build();
        //When
        Optional<DownMessage> outputMessage = jmesPathOperation.applyDownOperation(inputDownMessage, jmespath);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("update_command_default_input_jmespath_expected.json"));
        Optional<DownMessage> expectedOutputMessage = Optional.of(DownMessage.newDownMessageBuilder(inputDownMessage).command(Command.newCommandBuilder().id(expectedMessage.get("id").asText()).input(expectedMessage.get("input")).build()).build());
        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }
    @Test
    public void should_update_command_id_input_when_there_is_a_default_mismatch() throws PointExtractionException, IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("update_command_id.json");
        ObjectNode inputMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("update_command_id_input_default_jmespath.json"));
        Map<String, UpdateCommand> updateCommands = new HashMap<>();
        updateCommands.put("default", UpdateCommand.newUpdateCommandBuilder().id(inputMessage.get("id").asText()).input(inputMessage.get("input")).build());
        DownUpdateCommand jmespath = DownUpdateCommand.newDownUpdateCommandBuilder().commands(updateCommands).build();
        //When
        Optional<DownMessage> outputMessage = jmesPathOperation.applyDownOperation(inputDownMessage, jmespath);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("update_command_id_input_default_jmespath_expected.json"));
        Optional<DownMessage> expectedOutputMessage = Optional.of(DownMessage.newDownMessageBuilder(inputDownMessage).command(Command.newCommandBuilder().id(expectedMessage.get("id").asText()).input(expectedMessage.get("input")).build()).build());
        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }
    @Test
    public void should_update_command_id_when_there_is_a_default_match() throws PointExtractionException, IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("update_command_id.json");
        Map<String, UpdateCommand> updateCommands = new HashMap<>();
        updateCommands.put("myDeviceCommand", UpdateCommand.newUpdateCommandBuilder().id("newCommandId").build());
        updateCommands.put("default", UpdateCommand.newUpdateCommandBuilder().id("default").build());
        DownUpdateCommand jmespath = DownUpdateCommand.newDownUpdateCommandBuilder().commands(updateCommands).build();
        //When
        Optional<DownMessage> outputMessage = jmesPathOperation.applyDownOperation(inputDownMessage, jmespath);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("update_command_id_expected.json"));
        Optional<DownMessage> expectedOutputMessage = Optional.of(DownMessage.newDownMessageBuilder(inputDownMessage).command(Command.newCommandBuilder().id(expectedMessage.get("id").asText()).input(expectedMessage.get("input")).build()).build());
        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }
    @Test
    public void should_throw_exception_when_failed_to_extract_jmesexpression() throws PointExtractionException, IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("update_command_id.json");
        Map<String, UpdateCommand> updateCommands = new HashMap<>();
        updateCommands.put("myDeviceCommand", UpdateCommand.newUpdateCommandBuilder().input(jsonMapper.toJsonNode("\"{{ @.input.prop3}}\"")).build());
        DownUpdateCommand jmespath = DownUpdateCommand.newDownUpdateCommandBuilder().commands(updateCommands).build();
        //When & Then
        assertThatThrownBy(() -> jmesPathOperation.applyDownOperation(inputDownMessage, jmespath))
                .isInstanceOf(MessageExtractionException.class)
                .hasMessageContaining("retrieved value is null");
    }

}
