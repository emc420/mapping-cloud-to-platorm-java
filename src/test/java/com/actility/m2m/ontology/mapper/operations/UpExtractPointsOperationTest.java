package com.actility.m2m.ontology.mapper.operations;

import com.actility.m2m.commons.service.mapper.JsonMapper;
import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.actility.m2m.flow.data.*;
import com.actility.m2m.flow.data.Record;
import com.actility.m2m.ontology.mapper.PointExtractionException;
import com.actility.m2m.ontology.mapping.java.lib.data.JmesPathPoint;
import com.actility.m2m.ontology.mapping.java.lib.data.JmesPathPointType;
import com.actility.m2m.ontology.mapping.java.lib.data.UpExtractPoints;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.json.JsonArray;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Maps.newHashMap;

public class UpExtractPointsOperationTest {

    private static final JsonMapper jsonMapper = new JsonMapper(ObjectMapperModule.createObjectMapper());
    UpExtractPointsOperation jmesPathOperation = new UpExtractPointsOperation();

    private UpMessage buildInputUpMessage(String inputMessageFile, Map<String, Point> inputPoints) throws IOException {
        ObjectNode message =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream(inputMessageFile));
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
                .packet(MessagePacket.newMessagePacketBuilder().message(message).build())
                .build();
    }
    @Test
    public void should_exclude_points_when_value_is_null() throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("missing_point_value.json", new HashMap<>());

        JmesPathPoint humidity =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.humidity}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("humidity", humidity)).build();

        // When
        Optional<UpMessage> outputUpMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);

        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(new HashMap<>()).build();

        assertThat(outputUpMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_include_existing_points_from_input_message() throws PointExtractionException, IOException {
        // Given
        Map<String, Point> inputPoints = new HashMap<>();
        inputPoints.put(
                "Dummy", Point.newPointBuilder().unitId("D").type(PointType.STRING).records(new ArrayList<>()).build());
        UpMessage inputUpMessage = buildInputUpMessage("include_existing_points.json", inputPoints);
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
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
        expectedPoints.put(
                "Dummy", Point.newPointBuilder().unitId("D").type(PointType.STRING).records(new ArrayList<>()).build());
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_for_array_length_mismatch() throws IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("length_mismatch_value_event_time.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.measures[?id == 'temperature'].value}}")
                        .eventTime("{{packet.message.measures[?id == 'temperature'].time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When & Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality for 'value' and 'eventTime'");
    }

    @Test
    public void should_throw_exception_for_only_value_is_array_not_event_time() throws IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("only_value_is_array.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.measures[?id == 'temperature'].value}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When & Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality for 'value' and 'eventTime'");
    }

    @Test
    public void should_throw_exception_for_only_event_time_is_array_not_value() throws IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("only_event_time_is_array.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When & Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality for 'value' and 'eventTime'");
    }

    @Test
    public void should_throw_exception_for_event_time_is_null_and_value_not_null() throws IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("event_time_is_null_and_value_not_null.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When & Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality for 'value' and 'eventTime'");
    }

    @Test
    public void should_exclude_point_if_value_is_empty_array() throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("empty_value_array.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        Map<String, JmesPathPoint> points = new HashMap<>();
        points.put("temperature", temperature);
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(points).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPointMap = new HashMap<>();

        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPointMap).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_point_if_value_and_event_time_are_null() throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("null_value_event_time.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(new HashMap<>()).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_point_if_value_is_null_event_time_is_empty_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("null_value_empty_array_event_time.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(new HashMap<>()).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_point_if_value_is_empty_event_time_is_empty_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("value_empty_array_event_time_empty_array.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then

        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(new HashMap<>()).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_point_if_value_is_empty_event_time_is_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("value_empty_event_time_null.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(new HashMap<>()).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_if_value_is_not_null_event_time_empty_array() throws IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("value_not_null_event_time_empty.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When & Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality for 'value' and 'eventTime'");
    }

    @Test
    public void should_exclude_point_if_value_is_empty_event_time_is_not_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("value_is_empty_event_time_not_null.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(new HashMap<>()).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_if_value_is_array_event_time_empty_array() throws IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("value_array_event_time_empty.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When & Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality for 'value' and 'eventTime'");
    }

    @Test
    public void should_return_transformed_message_value_event_time_numbers()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("value_event_time_numbers.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
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
        UpMessage expectedOutputMessage =
                UpMessage.newUpMessageBuilder(inputUpMessage)
                        .points(newHashMap("temperature", expectedTemperature))
                        .build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_return_transformed_message_value_event_time_arrays_of_equal_length()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("value_event_time_equal_length_arrays.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        List<Record> expectedRecords =
                Arrays.asList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-02-06T09:14:05.688Z"))
                                .value(jsonMapper.toJsonNode(11.625))
                                .build(),
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-02-06T09:15:45.688Z"))
                                .value(jsonMapper.toJsonNode(11.625))
                                .build(),
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-02-06T09:17:25.688Z"))
                                .value(jsonMapper.toJsonNode(11.6875))
                                .build());
        Point expectedTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        UpMessage expectedOutputMessage =
                UpMessage.newUpMessageBuilder(inputUpMessage)
                        .points(newHashMap("temperature", expectedTemperature))
                        .build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_error_when_event_time_is_not_date_time() throws IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("event_time_is_not_date_time.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When & Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(DateTimeParseException.class)
                .hasMessageContaining("could not be parsed at");
    }

    @Test
    public void should_throw_error_when_single_element_in_event_time_array_is_not_date_time() throws IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("element_in_a_event_time_array_is_not_date_time.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When & Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(DateTimeParseException.class)
                .hasMessageContaining("could not be parsed at");
    }

    @Test
    public void should_include_point_when_value_is_array_of_equal_length_1_and_event_time_is_not_an_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("value_array_size_1_event_time_not_array.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.measures[?id == 'temperature'].value}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .value(jsonMapper.toJsonNode(11.625))
                                .build());
        Point expectedTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedPoints.put("temperature", expectedTemperature);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_include_point_when_event_time_is_array_of_equal_length_1_and_value_is_not_an_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("event_time_array_1_value_not_array.json", new HashMap<>());
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature.value}}")
                        .eventTime("{{packet.message.temperature.time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-02-06T09:14:05.688Z"))
                                .value(jsonMapper.toJsonNode(11.625))
                                .build());
        Point expectedTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedPoints.put("temperature", expectedTemperature);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_include_point_when_event_time_is_not_array_and_coordinate_is_not_an_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("coordinates_not_array_event_time_not_array.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{@ | date_time_op(time, '+', packet.message.age, 's')}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                                .coordinates(Arrays.asList(7.0586624, 43.6618752))
                                .build());
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_point_when_event_time_is_null_and_coordinate_is_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("coordinate_is_null_event_time_is_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_point_when_event_time_is_empty_and_coordinate_is_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("coordinate_is_null_event_time_is_empty.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_point_when_event_time_is_null_and_coordinate_is_empty()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("coordinate_is_empty_event_time_is_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_point_when_event_time_is_empty_and_coordinate_is_empty()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("coordinate_is_empty_event_time_is_empty.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_point_when_event_time_is_not_null_and_coordinate_is_empty()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("coordinate_is_empty_event_time_is_not_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_when_event_time_is_empty_and_coordinate_is_not_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("coordinate_is_not_null_event_time_is_empty.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'coordinates' and 'eventTime' fields");
    }

    @Test
    public void should_include_point_when_event_time_is_array_and_size_1_coordinate_is_not_an_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("coordinate_not_array_event_time_array_size_1.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                                .coordinates(Arrays.asList(7.0586624, 43.6618752))
                                .build());
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_when_event_time_is_array_size_not_1_and_coordinate_is_not_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("coordinate_not_array_event_time_is_array_size_not_1.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining(
                        "there is a mismatch in cardinality between 'coordinates' and 'eventTime' fields");
    }

    @Test
    public void should_include_point_when_event_time_is_not_array_and_coordinate_is_array_size_1()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("coordinate_is_array_size_1_event_time_is_not_array.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .coordinates(Arrays.asList(7.0586624, 43.6618752))
                                .build());
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_when_event_time_is_not_array_and_coordinate_is_array_size_not_1()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("coordinate_is_array_size_not_1_event_time_not_array.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'coordinates' and 'eventTime' fields");
    }

    @Test
    public void should_include_point_when_event_time_is_array_and_coordinate_is_array_equal_size()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("coordinates_array_and_size_equals_event_time_array.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        Record gpsRecord1 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                        .coordinates(Arrays.asList(7.0586624, 43.6618752))
                        .build();
        Record gpsRecord2 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                        .coordinates(Arrays.asList(7.0586624, 43.6618752))
                        .build();
        List<Record> expectedRecords = new ArrayList<>();
        expectedRecords.add(gpsRecord1);
        expectedRecords.add(gpsRecord2);
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_when_event_time_is_array_and_coordinate_is_array_size_not_equal()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("coordinate_array_event_time_array_size_not_equal.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'coordinates' and 'eventTime' fields");
    }

    @Test
    public void should_exclude_point_when_event_time_is_array_and_coordinate_is_empty()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("coordinate_is_empty_event_time_array.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_when_event_time_is_null_and_coordinate_is_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("coordinate_is_array_event_time_is_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'coordinates' and 'eventTime' fields");
    }

    @Test
    public void should_throw_exception_when_event_time_is_null_and_coordinate_is_not_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("coordinates_is_not_null_event_time_is_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'coordinates' and 'eventTime' fields");
    }

    @Test
    public void should_throw_exception_when_event_time_is_empty_and_coordinate_is_not_empty()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("coordinates_is_not_empty_event_time_is_empty.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'coordinates' and 'eventTime' fields");
    }

    @Test
    public void should_exclude_point_when_event_time_is_not_empty_and_coordinate_is_empty()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("coordinates_is_empty_event_time_not_empty.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_coordinate_when_lat_is_null_lng_is_null() throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_null_lng_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_coordinate_when_lat_is_null_lng_is_empty() throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_null_lng_empty.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_coordinate_when_lat_is_empty_lng_is_null() throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_empty_lng_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_when_lat_is_empty_and_lng_is_not_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_empty_lng_not_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_throw_exception_when_lat_is_not_null_and_lng_is_empty()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_not_null_lng_empty.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_include_point_when_lat_is_not_array_and_lng_is_array_size_1()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_not_array_lng_array_size_1.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .coordinates(Arrays.asList(7.0586624, 43.6618752))
                                .build());
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_when_lat_is_not_array_and_lng_is_array_size_not_1()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_not_array_lng_array_size_not_1.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'coordinates' and 'eventTime' fields");
    }

    @Test
    public void should_include_point_when_lat_is_array_size_1_and_lng_is_not_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_array_size_1_lng_not_array.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .coordinates(Arrays.asList(7.0586624, 43.6618752))
                                .build());
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_when_lat_is_array_size_not_1_and_lng_is_not_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_array_size_not_1_lng_is_not_array.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_throw_exception_when_lat_is_array_and_lng_is_array_unequal_size()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_array_lng_array_unequal_size.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_throw_exception_when_lat_is_null_and_lng_is_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_null_lng_array.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_throw_exception_when_lat_is_array_and_lng_is_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_array_lng_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_throw_exception_when_lat_is_null_and_lng_is_not_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_null_lng_not_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_throw_exception_when_lat_is_not_null_and_lng_is_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_is_not_null_lng_is_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_throw_exception_when_lat_is_not_empty_and_lng_is_empty()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_is_not_empty_lng_is_empty.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_throw_exception_when_lat_is_empty_and_lng_is_not_empty()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_is_empty_lng_is_not_empty.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_exclude_point_when_lat_is_null_and_lng_is_null_alt_is_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_null_lng_null_alt_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_point_when_lat_is_null_and_lng_is_null_alt_is_empty()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_null_lng_null_alt_empty.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_point_when_lat_is_empty_and_lng_is_empty_alt_is_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_empty_lng_empty_alt_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_point_when_lat_is_empty_and_lng_is_empty_alt_is_empty()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_empty_lng_empty_alt_empty.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_when_lat_not_null_and_lng_not_null_alt_is_empty()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_not_null_lng_not_null_alt_empty.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_include_point_when_lat_not_null_and_lng_not_null_alt_is_not_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_not_null_lng_not_null_alt_not_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .coordinates(Arrays.asList(7.0586624, 43.6618752, 2.1345677))
                                .build());
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_include_point_when_lat_not_null_and_lng_not_null_alt_is_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_not_null_lng_not_null_alt_array.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .coordinates(Arrays.asList(7.0586624, 43.6618752, 2.1345677))
                                .build());
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_when_lat_not_null_and_lng_not_null_alt_is_array_size_not_1()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("lat_not_null_lng_not_null_alt_array_size_1.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_include_point_when_lat_array_and_lng_array_size_1_alt_is_not_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("lat_array_and_lng_array_size_1_alt_not_array.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .coordinates(Arrays.asList(7.0586624, 43.6618752, 2.1345677))
                                .build());
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_when_lat_array_and_lng_array_size_not_1_alt_is_not_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("lat_array_and_lng_array_size_not_1_alt_is_not_array.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining(
                        "there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_include_point_when_lat_lng_is_array_and_alt_is_array_equal_size()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("lat_lng_is_array_and_alt_is_array_equal_size.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        Record gpsRecord1 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                        .coordinates(Arrays.asList(7.0586624, 43.6618752, 2.1345677))
                        .build();
        Record gpsRecord2 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                        .coordinates(Arrays.asList(7.0586624, 43.6618752, 2.1345677))
                        .build();
        List<Record> expectedRecords = new ArrayList<>();
        expectedRecords.add(gpsRecord1);
        expectedRecords.add(gpsRecord2);
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception_when_lat_lng_is_array_and_alt_is_array_unequal_size()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("lat_lng_is_array_and_alt_is_array_unequal_size.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_throw_exception_when_lat_array_and_lng_array_alt_is_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("lat_array_and_lng_array_alt_is_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_throw_exception_when_lat_not_null_and_lng_not_null_alt_is_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("lat_not_null_and_lng_not_null_alt_is_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder().coordinates(coordinate).eventTime("{{time}}").build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_throw_exception_when_lat_not_empty_and_lng_not_empty_alt_is_empty()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("lat_not_empty_and_lng_not_empty_alt_is_empty.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields");
    }

    @Test
    public void should_include_point_when_coordinate_is_not_null_value_is_not_null_event_time_is_not_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage(
                        "coordinate_is_not_null_value_is_not_null_event_time_is_not_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{time}}")
                        .value("{{packet.message.value}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .coordinates(Arrays.asList(7.0586624, 43.6618752))
                                .value(jsonMapper.toJsonNode(23))
                                .build());
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_include_point_when_coordinate_is_array_value_is_array_event_time_is_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage("coordinate_is_array_value_is_array_event_time_is_array.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .value("{{packet.message.value}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        Record gpsRecord1 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                        .coordinates(Arrays.asList(7.0586624, 43.6618752))
                        .value(jsonMapper.toJsonNode(23))
                        .build();
        Record gpsRecord2 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                        .coordinates(Arrays.asList(7.0586624, 43.6618752))
                        .value(jsonMapper.toJsonNode(23))
                        .build();
        List<Record> expectedRecords = new ArrayList<>();
        expectedRecords.add(gpsRecord1);
        expectedRecords.add(gpsRecord2);
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_throw_exception__when_coordinate_is_array_value_is_array_event_time_is_array_unequal_size()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage(
                        "coordinate_is_array_value_is_array_event_time_is_array_unequal_size.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .value("{{packet.message.value}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'coordinates' and 'eventTime' fields");
    }

    @Test
    public void should_include_point_when_coordinate_with_alt_is_not_null_value_is_not_null_event_time_is_not_null()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage(
                        "coordinate_with_alt_is_not_null_value_is_not_null_event_time_is_not_null.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{time}}")
                        .value("{{packet.message.value}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords =
                Collections.singletonList(
                        Record.newRecordBuilder()
                                .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                                .coordinates(Arrays.asList(7.0586624, 43.6618752, 2.1345677))
                                .value(jsonMapper.toJsonNode(23))
                                .build());
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_include_point_when_coordinate_with_alt_is_array_value_is_array_event_time_is_array()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage(
                        "coordinate_with_alt_is_array_value_is_array_event_time_is_array.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .value("{{packet.message.value}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When
        Optional<UpMessage> outputMessage = jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        Record gpsRecord1 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                        .coordinates(Arrays.asList(7.0586624, 43.6618752, 2.1345677))
                        .value(jsonMapper.toJsonNode(23))
                        .build();
        Record gpsRecord2 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                        .coordinates(Arrays.asList(7.0586624, 43.6618752, 2.1345677))
                        .value(jsonMapper.toJsonNode(23))
                        .build();
        List<Record> expectedRecords = new ArrayList<>();
        expectedRecords.add(gpsRecord1);
        expectedRecords.add(gpsRecord2);
        Point expectedCoordinates = Point.newPointBuilder().records(new ArrayList<>(expectedRecords)).build();
        expectedPoints.put("coordinates", expectedCoordinates);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void
    should_throw_exception_when_coordinate_with_alt_is_array_value_is_array_event_time_is_array_unequal_size()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage =
                buildInputUpMessage(
                        "coordinate_with_alt_is_array_value_is_array_event_time_is_array_unequal_size.json", new HashMap<>());
        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsAltitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .coordinates(coordinate)
                        .eventTime("{{packet.message.time}}")
                        .value("{{packet.message.value}}")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("coordinates", coordinates)).build();
        // When && Then
        assertThatThrownBy(() -> jmesPathOperation.applyUpOperation(inputUpMessage, jmesPath))
                .isInstanceOf(PointExtractionException.class)
                .hasMessageContaining("there is a mismatch in cardinality between 'coordinates' and 'eventTime' fields");
    }
    @Test
    public void should_include_ontology_id_existing_points() throws PointExtractionException, IOException {
        // Given
        Map<String, Point> inputPoints = new HashMap<>();
        inputPoints.put(
                "Dummy", Point.newPointBuilder().unitId("D").type(PointType.STRING).records(new ArrayList<>()).build());
        UpMessage inputUpMessage = buildInputUpMessage("ontology_id_existing_points.json", inputPoints);
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .ontologyId("temperature:1:value")
                        .value("{{packet.message.temperature}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(newHashMap("temperature", temperature)).build();
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
                        .ontologyId("temperature:1:value")
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();

        expectedPoints.put("temperature", expectedTemperature);
        expectedPoints.put(
                "Dummy", Point.newPointBuilder().unitId("D").type(PointType.STRING).records(new ArrayList<>()).build());
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }
}
