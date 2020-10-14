package com.actility.m2m.ontology.mapper.operations;

import com.actility.m2m.commons.service.mapper.JsonMapper;
import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.actility.m2m.flow.data.*;
import com.actility.m2m.ontology.mapper.MessageExtractionException;
import com.actility.m2m.ontology.mapping.java.lib.data.DownExtractDriverMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DownExtractDriverMessageOperationTest {

    private static final JsonMapper jsonMapper = new JsonMapper(ObjectMapperModule.createObjectMapper());
    DownExtractDriverMessageOperation jmesPathOperation = new DownExtractDriverMessageOperation();

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
    public void should_extract_message_downmessage_as_valid_jmespath_expression() throws IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("downmessage_sample.json");
        Map<String, JsonNode> commands = new HashMap<>();
        ObjectNode jmespathJson =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("downmessage_as_valid_jmespath_expression.json"));
        commands.put("myDeviceCommand", jsonMapper.toJsonNode(jmespathJson));
        DownExtractDriverMessage jmespath = DownExtractDriverMessage.newDownExtractDriverMessageBuilder().commands(commands).build();
        //When
        Optional<DownMessage> outputMessage = jmesPathOperation.applyDownOperation(inputDownMessage, jmespath);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("downmessage_as_valid_jmespath_expression_expected.json"));
        DownMessage expectedOutputMessage = DownMessage.newDownMessageBuilder(inputDownMessage).packet(MessagePacket.newMessagePacketBuilder().message(expectedMessage).build()).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);

    }
    @Test
    public void should_throw_exception_downmessage_as_invalid_jmespath_expression() throws IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("downmessage_sample.json");
        Map<String, JsonNode> commands = new HashMap<>();
        commands.put("myDeviceCommand", jsonMapper.toJsonNode("\"{{ command.type }}\""));
        DownExtractDriverMessage jmespath = DownExtractDriverMessage.newDownExtractDriverMessageBuilder().commands(commands).build();
        //When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyDownOperation(inputDownMessage, jmespath))
                .isInstanceOf(MessageExtractionException.class)
                .hasMessageContaining("expected object for 'message' but returned value node or null");

    }
    @Test
    public void should_throw_exception_downmessage_as_some_value() throws IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("downmessage_sample.json");
        Map<String, JsonNode> commands = new HashMap<>();
        commands.put("myDeviceCommand", jsonMapper.toJsonNode("\"somevalue\""));
        DownExtractDriverMessage jmespath = DownExtractDriverMessage.newDownExtractDriverMessageBuilder().commands(commands).build();
        //When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyDownOperation(inputDownMessage, jmespath))
                .isInstanceOf(MessageExtractionException.class)
                .hasMessageContaining("expected object but is a value node");

    }
    @Test
    public void should_throw_exception_downmessage_as_empty() throws IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("downmessage_sample.json");
        Map<String, JsonNode> commands = new HashMap<>();
        commands.put("myDeviceCommand", jsonMapper.toJsonNode("\"\""));
        DownExtractDriverMessage jmespath = DownExtractDriverMessage.newDownExtractDriverMessageBuilder().commands(commands).build();
        //When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyDownOperation(inputDownMessage, jmespath))
                .isInstanceOf(MessageExtractionException.class)
                .hasMessageContaining("expected object but is a value node");

    }
    @Test
    public void should_extract_default_jmesexpression_in_case_of_mismatch_message_downmessage() throws IOException {
        // Given
        ObjectNode jmespathJson =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("downmessage_as_default_jmespath_expression.json"));
        DownMessage inputDownMessage = buildInputDownMessage("downmessage_sample.json");
        Map<String, JsonNode> commands = new HashMap<>();
        commands.put("default", jsonMapper.toJsonNode(jmespathJson));
        DownExtractDriverMessage jmespath = DownExtractDriverMessage.newDownExtractDriverMessageBuilder().commands(commands).build();
        //When
        Optional<DownMessage> outputMessage = jmesPathOperation.applyDownOperation(inputDownMessage, jmespath);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("valid_jmesexpression_message_downmessage_expected.json"));
        DownMessage expectedOutputMessage = DownMessage.newDownMessageBuilder(inputDownMessage).packet(MessagePacket.newMessagePacketBuilder().message(expectedMessage).build()).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }
    @Test
    public void should_not_extract_any_command_in_case_of_message_mismatch_without_default_downmessage() throws IOException {
        // Given
        ObjectNode jmespathJson =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("downmessage_without_default_jmespath_expression.json"));
        DownMessage inputDownMessage = buildInputDownMessage("downmessage_sample.json");
        Map<String, JsonNode> commands = new HashMap<>();
        commands.put("setTransmissionFrameStatusPeriod", jsonMapper.toJsonNode(jmespathJson));
        DownExtractDriverMessage jmespath = DownExtractDriverMessage.newDownExtractDriverMessageBuilder().commands(commands).build();
        //When
        Optional<DownMessage> outputMessage = jmesPathOperation.applyDownOperation(inputDownMessage, jmespath);
        //Then
        DownMessage expectedOutputMessage = DownMessage.newDownMessageBuilder(inputDownMessage).packet(MessagePacket.newMessagePacketBuilder().build()).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }
    @Test
    public void should_extract_command_if_command_is_jmesexpression_downmessage() throws IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("downmessage_sample.json");
        Map<String, JsonNode> commands = new HashMap<>();
        commands.put("myDeviceCommand", jsonMapper.toJsonNode("\"{{ command.input }}\""));
        DownExtractDriverMessage jmespath = DownExtractDriverMessage.newDownExtractDriverMessageBuilder().commands(commands).build();
        //When
        Optional<DownMessage> outputMessage = jmesPathOperation.applyDownOperation(inputDownMessage, jmespath);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("downmessage_command_is_a_jmesexpression_expected.json"));
        DownMessage expectedOutputMessage = DownMessage.newDownMessageBuilder(inputDownMessage).packet(MessagePacket.newMessagePacketBuilder().message(expectedMessage).build()).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

}