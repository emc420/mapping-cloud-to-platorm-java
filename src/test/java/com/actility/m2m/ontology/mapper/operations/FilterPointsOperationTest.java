package com.actility.m2m.ontology.mapper.operations;

import com.actility.m2m.commons.service.mapper.JsonMapper;
import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.actility.m2m.flow.data.*;
import com.actility.m2m.flow.data.Record;
import com.actility.m2m.ontology.mapper.PointExtractionException;
import com.actility.m2m.ontology.mapping.java.lib.data.UpFilterPointsOperation;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Maps.newHashMap;

public class FilterPointsOperationTest {

    private static final JsonMapper jsonMapper = new JsonMapper(ObjectMapperModule.createObjectMapper());
    FilterPointsOperation filterPointsOperation = new FilterPointsOperation();

    private UpMessage buildInputUpMessage(Map<String, Point> inputPoints) throws IOException {
        return UpMessage.newUpMessageBuilder()
                .id("00000000-000000-00000-000000000")
                .time(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                .subAccount(Account.newAccountBuilder().id("subAccount1").realmId("subRealm1").build())
                .origin(
                        UpOrigin.newUpOriginBuilder().id("tpw").type(UpOriginType.BINDER).time(OffsetDateTime.now()).build())
                .content(JsonNodeFactory.instance.objectNode())
                .type(UpMessageType.DEVICEUPLINK)
                .points(inputPoints)
                .thing(com.actility.m2m.flow.data.Thing.newThingBuilder().key("lora:0102030405060708").build())
                .subscriber(
                        com.actility.m2m.flow.data.Subscriber.newSubscriberBuilder().id("sub1").realmId("realm1").build())
                .build();
    }
    @Test
    public void should_be_unchanged_message_when_points_filter_is_empty_message_points_empty() throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(new HashMap<>());
        List<String> points = new ArrayList<>();
        UpFilterPointsOperation upFilterPoints= UpFilterPointsOperation.newUpFilterPointsOperationBuilder().points(points).build();

        // When
        Optional<UpMessage> outputUpMessage = filterPointsOperation.applyUpOperation(inputUpMessage, upFilterPoints);

        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();

        assertThat(outputUpMessage).hasValue(expectedOutputMessage);
    }
    @Test
    public void should_return_empty_points_message_when_points_filter_is_empty_message_points_not_empty() throws PointExtractionException, IOException {
        // Given
        Map<String, Point> inputPoints = new HashMap<>();
        List<Record> inputRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .value(jsonMapper.toJsonNode(22.6))
                                .build());
        Point inputTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(inputRecords))
                        .build();

        inputPoints.put("temperature", inputTemperature);
        UpMessage inputUpMessage = buildInputUpMessage(inputPoints);
        List<String> points = new ArrayList<>();
        UpFilterPointsOperation upFilterPoints= UpFilterPointsOperation.newUpFilterPointsOperationBuilder().points(points).build();

        // When
        Optional<UpMessage> outputUpMessage = filterPointsOperation.applyUpOperation(inputUpMessage, upFilterPoints);

        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(new HashMap<>()).build();

        assertThat(outputUpMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_be_unchanged_message_when_points_filter_is_not_empty_message_points_empty() throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(new HashMap<>());
        List<String> points = new ArrayList<>();
        points.add("temperature");
        points.add("humidity");
        UpFilterPointsOperation upFilterPoints= UpFilterPointsOperation.newUpFilterPointsOperationBuilder().points(points).build();

        // When
        Optional<UpMessage> outputUpMessage = filterPointsOperation.applyUpOperation(inputUpMessage, upFilterPoints);

        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();

        assertThat(outputUpMessage).hasValue(expectedOutputMessage);
    }
    @Test
    public void should_be_unchanged_message_when_points_filter_is_not_empty_message_points_not_empty_match() throws PointExtractionException, IOException {
        // Given
        Map<String, Point> inputPoints = new HashMap<>();
        List<Record> inputRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .value(jsonMapper.toJsonNode(22.6))
                                .build());
        Point inputTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(inputRecords))
                        .build();

        inputPoints.put("temperature", inputTemperature);
        UpMessage inputUpMessage = buildInputUpMessage(inputPoints);
        List<String> points = new ArrayList<>();
        points.add("temperature");
        points.add("humidity");
        UpFilterPointsOperation upFilterPoints= UpFilterPointsOperation.newUpFilterPointsOperationBuilder().points(points).build();

        // When
        Optional<UpMessage> outputUpMessage = filterPointsOperation.applyUpOperation(inputUpMessage, upFilterPoints);

        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();

        assertThat(outputUpMessage).hasValue(expectedOutputMessage);
    }
    @Test
    public void should_return_empty_points_when_points_filter_is_not_empty_message_points_not_empty_not_match() throws PointExtractionException, IOException {
        // Given
        Map<String, Point> inputPoints = new HashMap<>();
        List<Record> inputRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .value(jsonMapper.toJsonNode(22.6))
                                .build());
        Point inputTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(inputRecords))
                        .build();

        inputPoints.put("temperature", inputTemperature);
        UpMessage inputUpMessage = buildInputUpMessage(inputPoints);
        List<String> points = new ArrayList<>();
        points.add("humidity");
        UpFilterPointsOperation upFilterPoints= UpFilterPointsOperation.newUpFilterPointsOperationBuilder().points(points).build();

        // When
        Optional<UpMessage> outputUpMessage = filterPointsOperation.applyUpOperation(inputUpMessage, upFilterPoints);

        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(new HashMap<>()).build();

        assertThat(outputUpMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_return_filtered_points_when_points_filter_has_multiple_entries_message_points_not_empty_selective_match() throws PointExtractionException, IOException {
        // Given
        Map<String, Point> inputPoints = new HashMap<>();
        List<Record> inputRecordsTemperature =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .value(jsonMapper.toJsonNode(22.6))
                                .build());

        List<Record> inputRecordsBatteryLevel =
                Collections.singletonList(
                                Record.newRecordBuilder()
                                        .eventTime(OffsetDateTime.parse("2020-02-06T09:18:15.688Z"))
                                        .value(jsonMapper.toJsonNode(30))
                                        .build());
        Point inputTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(inputRecordsTemperature))
                        .build();

        Point inputBatteryCurrentLevel =
                Point.newPointBuilder()
                        .unitId("%RH")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(inputRecordsBatteryLevel))
                        .build();

        inputPoints.put("temperature", inputTemperature);
        inputPoints.put("battery_current_level", inputBatteryCurrentLevel);
        UpMessage inputUpMessage = buildInputUpMessage(inputPoints);
        List<String> points = new ArrayList<>();
        points.add("battery_current_level");
        points.add("coordinates");
        points.add("batteryLevel");
        points.add("batteryStatus");
        UpFilterPointsOperation upFilterPoints= UpFilterPointsOperation.newUpFilterPointsOperationBuilder().points(points).build();

        // When
        Optional<UpMessage> outputUpMessage = filterPointsOperation.applyUpOperation(inputUpMessage, upFilterPoints);

        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(newHashMap("battery_current_level", inputBatteryCurrentLevel)).build();

        assertThat(outputUpMessage).hasValue(expectedOutputMessage);
    }
}
