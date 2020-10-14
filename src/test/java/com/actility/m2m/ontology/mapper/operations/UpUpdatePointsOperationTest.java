package com.actility.m2m.ontology.mapper.operations;

import com.actility.m2m.commons.service.mapper.JsonMapper;
import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.actility.m2m.flow.data.*;
import com.actility.m2m.flow.data.Record;
import com.actility.m2m.ontology.mapper.PointExtractionException;
import com.actility.m2m.ontology.mapping.java.lib.data.JmesPathPointType;
import com.actility.m2m.ontology.mapping.java.lib.data.UpdatePoint;
import com.actility.m2m.ontology.mapping.java.lib.data.UpUpdatePoints;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.vertx.core.json.JsonArray;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Maps.newHashMap;

public class UpUpdatePointsOperationTest {

    private static final JsonMapper jsonMapper = new JsonMapper(ObjectMapperModule.createObjectMapper());
    UpUpdatePointsOperation jmesPathOperation = new UpUpdatePointsOperation();

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
    public void should_update_type_from_matched_points_from_update_points() throws PointExtractionException, IOException {
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
        UpdatePoint temperature =
                UpdatePoint.newUpdatePointBuilder()
                        .type(JmesPathPointType.STRING)
                        .build();
        UpUpdatePoints jmesPath = UpUpdatePoints.newUpUpdatePointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .value(jsonMapper.toJsonNode(22.6))
                                .build());
        Point expectedTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.STRING)
                        .records(new ArrayList<>(expectedRecords))
                        .build();

        expectedPoints.put("temperature", expectedTemperature);
        Optional<UpMessage> expectedOutputMessage = Optional.of(UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build());

        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }

    @Test
    public void should_update_unit_id_from_matched_points_from_update_points() throws PointExtractionException, IOException {
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
        UpdatePoint temperature =
                UpdatePoint.newUpdatePointBuilder()
                        .unitId("Far")
                        .build();
        UpUpdatePoints jmesPath = UpUpdatePoints.newUpUpdatePointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .value(jsonMapper.toJsonNode(22.6))
                                .build());
        Point expectedTemperature =
                Point.newPointBuilder()
                        .unitId("Far")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();

        expectedPoints.put("temperature", expectedTemperature);
        Optional<UpMessage> expectedOutputMessage = Optional.of(UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build());

        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }
    @Test
    public void should_update_value_from_matched_points_from_update_points() throws PointExtractionException, IOException {
        // Given
        Map<String, Point> inputPoints = new HashMap<>();
        List<Record> inputRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .value(jsonMapper.toJsonNode(1))
                                .build());
        Point inputTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(inputRecords))
                        .build();

        inputPoints.put("temperature", inputTemperature);
        UpMessage inputUpMessage = buildInputUpMessage(inputPoints);
        UpdatePoint temperature =
                UpdatePoint.newUpdatePointBuilder()
                        .type(JmesPathPointType.BOOLEAN)
                        .value("{{ @ | to_boolean(@) }}")
                        .build();
        UpUpdatePoints jmesPath = UpUpdatePoints.newUpUpdatePointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .value(jsonMapper.toJsonNode(true))
                                .build());
        Point expectedTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.BOOLEAN)
                        .records(new ArrayList<>(expectedRecords))
                        .build();

        expectedPoints.put("temperature", expectedTemperature);
        Optional<UpMessage> expectedOutputMessage = Optional.of(UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build());

        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }

    @Test
    public void should_update_event_time_from_matched_points_from_update_points() throws PointExtractionException, IOException {
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
        UpdatePoint temperature =
                UpdatePoint.newUpdatePointBuilder()
                        .eventTime("{{@ | date_time_op(@, '+', '5' , 's')}} ")
                        .build();
        UpUpdatePoints jmesPath = UpUpdatePoints.newUpUpdatePointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                                .value(jsonMapper.toJsonNode(22.6))
                                .build());
        Point expectedTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();

        expectedPoints.put("temperature", expectedTemperature);
        Optional<UpMessage> expectedOutputMessage = Optional.of(UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build());

        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }

    @Test
    public void should_update_ontology_id_from_matched_points_from_update_points() throws PointExtractionException, IOException {
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
        UpdatePoint temperature =
                UpdatePoint.newUpdatePointBuilder()
                        .ontologyId("temperatureMeasurement:1:temperature")
                        .build();
        UpUpdatePoints jmesPath = UpUpdatePoints.newUpUpdatePointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .value(jsonMapper.toJsonNode(22.6))
                                .build());
        Point expectedTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .ontologyId("temperatureMeasurement:1:temperature")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();

        expectedPoints.put("temperature", expectedTemperature);
        Optional<UpMessage> expectedOutputMessage = Optional.of(UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build());

        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }

    @Test
    public void should_update_coordinate_from_matched_points_from_update_points()
            throws PointExtractionException, IOException {
        // Given
        Map<String, Point> inputPoints = new HashMap<>();
        List<Record> inputRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .coordinates(Arrays.asList(7.0586624, 43.6618752))
                                .build());
        Point inputCoordinates = Point.newPointBuilder().records(new ArrayList<>(inputRecords)).build();
        inputPoints.put("coordinates", inputCoordinates);
        UpMessage inputUpMessage =
                buildInputUpMessage(inputPoints);
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{@ | floor(@)}}");
        coordinate.add(
                "{{@ | floor(@)}}");
        UpdatePoint coordinates =
                UpdatePoint.newUpdatePointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{@ | date_time_op(@, '+', '5', 's')}}")
                        .build();
        UpUpdatePoints jmesPath = UpUpdatePoints.newUpUpdatePointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                                .coordinates(Arrays.asList(7.0, 43.0))
                                .build());
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        Optional<UpMessage> expectedOutputMessage = Optional.of(UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build());

        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }

    @Test
    public void should_update_coordinate_altitude_from_matched_points_from_update_points()
            throws PointExtractionException, IOException {
        // Given
        Map<String, Point> inputPoints = new HashMap<>();
        List<Record> inputRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .coordinates(Arrays.asList(7.0586624, 43.6618752, 35.675858))
                                .build());
        Point inputCoordinates = Point.newPointBuilder().records(new ArrayList<>(inputRecords)).build();
        inputPoints.put("coordinates", inputCoordinates);
        UpMessage inputUpMessage =
                buildInputUpMessage(inputPoints);
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{@ | floor(@)}}");
        coordinate.add(
                "{{@ | floor(@)}}");
        coordinate.add(
                "{{@ | floor(@)}}");
        UpdatePoint coordinates =
                UpdatePoint.newUpdatePointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{@ | date_time_op(@, '+', '5', 's')}}")
                        .build();
        UpUpdatePoints jmesPath = UpUpdatePoints.newUpUpdatePointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                                .coordinates(Arrays.asList(7.0, 43.0, 35.0))
                                .build());
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        Optional<UpMessage> expectedOutputMessage = Optional.of(UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build());

        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }

    @Test
    public void should_not_update_when_there_is_a_mismatch_points_from_update_points() throws PointExtractionException, IOException {
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
        UpdatePoint humidity =
                UpdatePoint.newUpdatePointBuilder()
                        .unitId("%RH")
                        .build();
        UpUpdatePoints jmesPath = UpUpdatePoints.newUpUpdatePointsBuilder().points(newHashMap("humidity", humidity)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .value(jsonMapper.toJsonNode(22.6))
                                .build());
        Point expectedTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();

        expectedPoints.put("temperature", expectedTemperature);
        Optional<UpMessage> expectedOutputMessage = Optional.of(UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build());

        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }
}
