package com.actility.m2m.ontology.mapper;

import com.actility.m2m.commons.service.mapper.JsonMapper;
import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.actility.m2m.flow.data.*;
import com.actility.m2m.flow.data.Record;
import com.actility.m2m.ontology.mapping.java.lib.data.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.json.JsonArray;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class OntologyMappingTest {

    private static final JsonMapper jsonMapper = new JsonMapper(ObjectMapperModule.createObjectMapper());
    OperationService oprServ;
    OperationFactory operationFactory;

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

    @BeforeEach
    public void setup() {
        operationFactory = new OperationFactory();
        oprServ = new OperationService(operationFactory);
    }

    @Test
    public void should_extract_elsys_points() throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("elsys.json", new HashMap<>());
        List<UpOperation> operations = new ArrayList<>();
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.temperature}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        JmesPathPoint humidity =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.humidity}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("%RH")
                        .build();
        JmesPathPoint light =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.light}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("lx")
                        .build();
        Map<String, JmesPathPoint> outputPoints = new HashMap<>();
        outputPoints.put("temperature", temperature);
        outputPoints.put("humidity", humidity);
        outputPoints.put("light", light);
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(outputPoints).build();
        operations.add(jmesPath);
        // When
        Optional<UpMessage> outputMessage = oprServ.applyUpOperations(inputUpMessage, operations);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        Record expectedTemperatureRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                        .value(jsonMapper.toJsonNode(22.6))
                        .build();
        Record expectedHumidityRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                        .value(jsonMapper.toJsonNode(41))
                        .build();
        Record expectedLightRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                        .value(jsonMapper.toJsonNode(39))
                        .build();
        List<Record> expectedRecords = new ArrayList<>();
        expectedRecords.add(expectedTemperatureRecord);
        Point expectedTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedRecords.clear();
        expectedRecords.add(expectedHumidityRecord);
        Point expectedHumidity =
                Point.newPointBuilder()
                        .unitId("%RH")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedRecords.clear();
        expectedRecords.add(expectedLightRecord);
        Point expectedLight =
                Point.newPointBuilder()
                        .unitId("lx")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedPoints.put("temperature", expectedTemperature);
        expectedPoints.put("humidity", expectedHumidity);
        expectedPoints.put("light", expectedLight);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_extract_sensing_labs_points()
            throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("sensing_labs.json", new HashMap<>());
        List<UpOperation> operations = new ArrayList<>();
        JmesPathPoint temperature =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.measures[?id == 'temperature'].value}}")
                        .eventTime("{{packet.message.measures[?id == 'temperature'].time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        JmesPathPoint batteryCurrentLevel =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.measures[?id == 'battery_current_level'].value}}")
                        .eventTime("{{packet.message.measures[?id == 'battery_current_level'].time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("%RH")
                        .build();

        Map<String, JmesPathPoint> points = new HashMap<>();
        points.put("temperature", temperature);
        points.put("battery_current_level", batteryCurrentLevel);
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(points).build();
        operations.add(jmesPath);
        // When
        Optional<UpMessage> outputMessage = oprServ.applyUpOperations(inputUpMessage, operations);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        Record expectedTemperatureRecord1 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-02-06T09:14:05.688Z"))
                        .value(jsonMapper.toJsonNode(11.625))
                        .build();
        Record expectedTemperatureRecord2 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-02-06T09:15:45.688Z"))
                        .value(jsonMapper.toJsonNode(11.625))
                        .build();
        Record expectedTemperatureRecord3 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-02-06T09:17:25.688Z"))
                        .value(jsonMapper.toJsonNode(11.6875))
                        .build();
        Record expectedBatteryCurrentLevelRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-02-06T09:18:15.688Z"))
                        .value(jsonMapper.toJsonNode(30))
                        .build();
        List<Record> expectedRecords = new ArrayList<>();
        expectedRecords.add(expectedTemperatureRecord1);
        expectedRecords.add(expectedTemperatureRecord2);
        expectedRecords.add(expectedTemperatureRecord3);
        Point expectedTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedRecords.clear();
        expectedRecords.add(expectedBatteryCurrentLevelRecord);
        Point expectedBatteryCurrentLevel =
                Point.newPointBuilder()
                        .unitId("%RH")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedPoints.put("temperature", expectedTemperature);
        expectedPoints.put("battery_current_level", expectedBatteryCurrentLevel);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_extract_nke_points() throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("nke.json", new HashMap<>());
        List<UpOperation> operations = new ArrayList<>();

        JmesPathPoint energy =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value(
                                "{{packet.message | to_array(@) | [?CommandID == 'ReportAttributes' && AttributeID == 'Attribute_0'].Data.TICFieldList.BBRHCJB | @[0]}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("W")
                        .build();

        Map<String, JmesPathPoint> points = new HashMap<>();
        points.put("energy", energy);
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(points).build();
        operations.add(jmesPath);
        // When
        Optional<UpMessage> outputMessage = oprServ.applyUpOperations(inputUpMessage, operations);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords = new ArrayList<>();
        Record expectedEnergyRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                        .value(jsonMapper.toJsonNode(123456789))
                        .build();
        expectedRecords.add(expectedEnergyRecord);
        Point expectedEnergy =
                Point.newPointBuilder()
                        .unitId("W")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedPoints.put("energy", expectedEnergy);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_extract_abeeway_points() throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("abeeway.json", new HashMap<>());
        List<UpOperation> operations = new ArrayList<>();

        JsonArray coordinate = new JsonArray();
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLongitude| [0]}}");
        coordinate.add(
                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].gpsLatitude | [0]}}");
        JmesPathPoint coordinates =
                JmesPathPoint.newJmesPathPointBuilder()
                        .ontologyId("Geolocation:1:coordinates")
                        .coordinates(coordinate)
                        .eventTime("{{@ | date_time_op(time, '+', packet.message.age, 's')}}")
                        .build();
        JmesPathPoint temperatureMeasure =
                JmesPathPoint.newJmesPathPointBuilder()
                        .ontologyId("TemperatureMeasurement:1:measuredValue")
                        .value("{{packet.message.temperatureMeasure}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("Cel")
                        .build();
        JmesPathPoint rawPositionType =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE'].rawPositionType | [0]}}")
                        .eventTime("{{@ | date_time_op(time, '+', packet.message.age, 's')}}")
                        .type(JmesPathPointType.STRING)
                        .build();
        JmesPathPoint batteryStatus =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.batteryStatus}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.STRING)
                        .build();

        JmesPathPoint horizontalAccuracy =
                JmesPathPoint.newJmesPathPointBuilder()
                        .ontologyId("Geolocation:1:horizontalAccuracy")
                        .value(
                                "{{packet.message | to_array(@) | [?messageType == 'POSITION_MESSAGE' && rawPositionType == 'GPS'].horizontalAccuracy | [0]}}")
                        .eventTime("{{@ | date_time_op(time, '+', packet.message.age, 's')}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("m")
                        .build();
        JmesPathPoint trackingMode =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.trackingMode}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.STRING)
                        .build();
        JmesPathPoint batteryLevel =
                JmesPathPoint.newJmesPathPointBuilder()
                        .ontologyId("PowerConfiguration:1:batteryPercentageRemaining")
                        .value("{{packet.message.batteryLevel}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("%")
                        .build();
        JmesPathPoint sosFlag =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.sosFlag | to_boolean(@)}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.BOOLEAN)
                        .build();
        JmesPathPoint dynamicMotionState =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value("{{packet.message.dynamicMotionState}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.STRING)
                        .build();
        Map<String, JmesPathPoint> points = new HashMap<>();
        points.put("coordinates", coordinates);
        points.put("horizontalAccuracy", horizontalAccuracy);
        points.put("rawPositionType", rawPositionType);
        points.put("trackingMode", trackingMode);
        points.put("batteryLevel", batteryLevel);
        points.put("batteryStatus", batteryStatus);
        points.put("temperatureMeasure", temperatureMeasure);
        points.put("sosFlag", sosFlag);
        points.put("dynamicMotionState", dynamicMotionState);
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(points).build();
        operations.add(jmesPath);
        // When
        Optional<UpMessage> outputMessage = oprServ.applyUpOperations(inputUpMessage, operations);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords = new ArrayList<>();
        Record expectedCoordinatesRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                        .coordinates(Arrays.asList(7.0586624, 43.6618752))
                        .build();
        expectedRecords.add(expectedCoordinatesRecord);
        Point expectedCoordinates = Point.newPointBuilder().ontologyId("Geolocation:1:coordinates").records(new ArrayList<>(expectedRecords)).build();
        expectedRecords.clear();
        expectedPoints.put("coordinates", expectedCoordinates);
        Record expectedHorizontalAccuracyRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                        .value(jsonMapper.toJsonNode(19))
                        .build();
        expectedRecords.add(expectedHorizontalAccuracyRecord);
        Point expectedHorizontalAccuracy =
                Point.newPointBuilder()
                        .ontologyId("Geolocation:1:horizontalAccuracy")
                        .unitId("m")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedRecords.clear();
        expectedPoints.put("horizontalAccuracy", expectedHorizontalAccuracy);
        Record expectedRawPositionTypeRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:05.000Z"))
                        .value(jsonMapper.toJsonNode("\"GPS\""))
                        .build();
        expectedRecords.add(expectedRawPositionTypeRecord);
        Point expectedRawPositionType =
                Point.newPointBuilder().type(PointType.STRING).records(new ArrayList<>(expectedRecords)).build();
        expectedRecords.clear();
        expectedPoints.put("rawPositionType", expectedRawPositionType);
        Record expectedTrackingModeRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                        .value(jsonMapper.toJsonNode("\"PERMANENT_TRACKING\""))
                        .build();
        expectedRecords.add(expectedTrackingModeRecord);
        Point expectedTrackingMode =
                Point.newPointBuilder().type(PointType.STRING).records(new ArrayList<>(expectedRecords)).build();
        expectedRecords.clear();
        expectedPoints.put("trackingMode", expectedTrackingMode);
        Record expectedBatteryLevelRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                        .value(jsonMapper.toJsonNode(95))
                        .build();
        expectedRecords.add(expectedBatteryLevelRecord);
        Point expectedBatteryLevel =
                Point.newPointBuilder()
                        .ontologyId("PowerConfiguration:1:batteryPercentageRemaining")
                        .unitId("%")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedRecords.clear();
        expectedPoints.put("batteryLevel", expectedBatteryLevel);
        Record expectedBatteryStatusRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                        .value(jsonMapper.toJsonNode("\"OPERATING\""))
                        .build();
        expectedRecords.add(expectedBatteryStatusRecord);
        Point expectedBatteryStatus =
                Point.newPointBuilder().type(PointType.STRING).records(new ArrayList<>(expectedRecords)).build();
        expectedRecords.clear();
        expectedPoints.put("batteryStatus", expectedBatteryStatus);
        Record expectedTemperatureMeasureRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                        .value(jsonMapper.toJsonNode(25.8))
                        .build();
        expectedRecords.add(expectedTemperatureMeasureRecord);
        Point expectedTemperatureMeasure =
                Point.newPointBuilder()
                        .ontologyId("TemperatureMeasurement:1:measuredValue")
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedRecords.clear();
        expectedPoints.put("temperatureMeasure", expectedTemperatureMeasure);
        Record expectedSosFlagRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                        .value(jsonMapper.toJsonNode(true))
                        .build();
        expectedRecords.add(expectedSosFlagRecord);
        Point expectedSosFlag =
                Point.newPointBuilder().type(PointType.BOOLEAN).records(new ArrayList<>(expectedRecords)).build();
        expectedRecords.clear();
        expectedPoints.put("sosFlag", expectedSosFlag);
        Record expectedDynamicMotionStateRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                        .value(jsonMapper.toJsonNode("\"STATIC\""))
                        .build();
        expectedRecords.add(expectedDynamicMotionStateRecord);
        Point expectedDynamicMotionState =
                Point.newPointBuilder().type(PointType.STRING).records(new ArrayList<>(expectedRecords)).build();
        expectedRecords.clear();
        expectedPoints.put("dynamicMotionState", expectedDynamicMotionState);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }
    @Test
    public void should_extract_adeunis_downmessage() throws PointExtractionException, IOException {
        // Given
        ObjectNode jmespathJson =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("adeunis_jmespath.json"));
        DownMessage inputDownMessage = buildInputDownMessage("adeunis.json");
        Map<String, JsonNode> commands = new HashMap<>();
        commands.put("setTransmissionFrameStatusPeriod", jsonMapper.toJsonNode(jmespathJson));
        DownExtractDriverMessage jmespath = DownExtractDriverMessage.newDownExtractDriverMessageBuilder().commands(commands).build();
        //When
        Optional<DownMessage> outputMessage = oprServ.applyDownOperations(inputDownMessage, Collections.singletonList(jmespath));
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("adeunis_expected_output.json"));
        DownMessage expectedOutputMessage = DownMessage.newDownMessageBuilder(inputDownMessage).packet(MessagePacket.newMessagePacketBuilder().message(expectedMessage).build()).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);

    }
    @Test
    public void should_extract_theoritical_downmessage() throws PointExtractionException, IOException {
        // Given
        DownMessage inputDownMessage = buildInputDownMessage("theoritical.json");
        Map<String, JsonNode> commands = new HashMap<>();
        commands.put("myDeviceCommand", jsonMapper.toJsonNode("\"{{ command | add_property(@.input, 'type', @.id) }}\""));
        DownExtractDriverMessage jmespath = DownExtractDriverMessage.newDownExtractDriverMessageBuilder().commands(commands).build();
        //When
        Optional<DownMessage> outputMessage = oprServ.applyDownOperations(inputDownMessage, Collections.singletonList(jmespath));
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("theoritical_expected_output.json"));
        DownMessage expectedOutputMessage = DownMessage.newDownMessageBuilder(inputDownMessage).packet(MessagePacket.newMessagePacketBuilder().message(expectedMessage).build()).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);

    }

    @Test
    public void should_execute_both_jmespath_filter_operations() throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("nke.json", new HashMap<>());
        List<UpOperation> operations = new ArrayList<>();

        JmesPathPoint energy =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value(
                                "{{packet.message | to_array(@) | [?CommandID == 'ReportAttributes' && AttributeID == 'Attribute_0'].Data.TICFieldList.BBRHCJB | @[0]}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("W")
                        .build();

        Map<String, JmesPathPoint> points = new HashMap<>();
        points.put("energy", energy);
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(points).build();

        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceUplink(true)
                .build();

        operations.add(jmesPath);
        operations.add(upFilterOperation);
        // When
        Optional<UpMessage> outputMessage = oprServ.applyUpOperations(inputUpMessage, operations);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords = new ArrayList<>();
        Record expectedEnergyRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                        .value(jsonMapper.toJsonNode(123456789))
                        .build();
        expectedRecords.add(expectedEnergyRecord);
        Point expectedEnergy =
                Point.newPointBuilder()
                        .unitId("W")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedPoints.put("energy", expectedEnergy);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }
    @Test
    public void should_extract_final_message_after_jmespath_filter_point_operations() throws PointExtractionException, IOException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage("nke.json", new HashMap<>());
        List<UpOperation> operations = new ArrayList<>();

        JmesPathPoint energy =
                JmesPathPoint.newJmesPathPointBuilder()
                        .value(
                                "{{packet.message | to_array(@) | [?CommandID == 'ReportAttributes' && AttributeID == 'Attribute_0'].Data.TICFieldList.BBRHCJB | @[0]}}")
                        .eventTime("{{time}}")
                        .type(JmesPathPointType.DOUBLE)
                        .unitId("W")
                        .build();

        Map<String, JmesPathPoint> points = new HashMap<>();
        points.put("energy", energy);
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(points).build();
        List<String> pointsFilters = new ArrayList<>();
        pointsFilters.add("energy");
        UpFilterPointsOperation upFilterPoints= UpFilterPointsOperation.newUpFilterPointsOperationBuilder().points(pointsFilters).build();
        operations.add(jmesPath);
        operations.add(upFilterPoints);
        // When
        Optional<UpMessage> outputMessage = oprServ.applyUpOperations(inputUpMessage, operations);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        List<Record> expectedRecords = new ArrayList<>();
        Record expectedEnergyRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                        .value(jsonMapper.toJsonNode(123456789))
                        .build();
        expectedRecords.add(expectedEnergyRecord);
        Point expectedEnergy =
                Point.newPointBuilder()
                        .unitId("W")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedPoints.put("energy", expectedEnergy);
        Optional<UpMessage> expectedOutputMessage = Optional.of(UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build());

        assertThat(outputMessage).isEqualTo(expectedOutputMessage);
    }
    @Test
    public void should_update_sensing_labs_points()
            throws PointExtractionException, IOException {
        // Given
        Map<String, Point> inputPoints = new HashMap<>();
        Record inputTemperatureRecord1 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-02-06T09:14:05.688Z"))
                        .value(jsonMapper.toJsonNode(11.625))
                        .build();
        Record inputTemperatureRecord2 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-02-06T09:15:45.688Z"))
                        .value(jsonMapper.toJsonNode(11.625))
                        .build();
        Record inputTemperatureRecord3 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-02-06T09:17:25.688Z"))
                        .value(jsonMapper.toJsonNode(11.6875))
                        .build();
        Record inputBatteryCurrentLevelRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-02-06T09:18:15.688Z"))
                        .value(jsonMapper.toJsonNode(30.5656))
                        .build();
        List<Record> inputRecords = new ArrayList<>();
        inputRecords.add(inputTemperatureRecord1);
        inputRecords.add(inputTemperatureRecord2);
        inputRecords.add(inputTemperatureRecord3);
        Point inputTemperature =
                Point.newPointBuilder()
                        .unitId("Cel")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(inputRecords))
                        .build();
        inputRecords.clear();
        inputRecords.add(inputBatteryCurrentLevelRecord);
        Point inputBatteryCurrentLevel =
                Point.newPointBuilder()
                        .unitId("%RH")
                        .type(PointType.DOUBLE)
                        .records(new ArrayList<>(inputRecords))
                        .build();
        inputPoints.put("temperature", inputTemperature);
        inputPoints.put("battery_current_level", inputBatteryCurrentLevel);
        UpMessage inputUpMessage = buildInputUpMessage("sensing_labs.json", inputPoints);
        List<UpOperation> operations = new ArrayList<>();
        UpdatePoint temperature =
                UpdatePoint.newUpdatePointBuilder()
                        .value("{{@ | floor(@)}}")
                        .type(JmesPathPointType.INT64)
                        .unitId("Far")
                        .build();
        UpdatePoint batteryCurrentLevel =
                UpdatePoint.newUpdatePointBuilder()
                        .value("{{@ | floor(@)}}")
                        .type(JmesPathPointType.INT64)
                        .build();

        Map<String, UpdatePoint> points = new HashMap<>();
        points.put("temperature", temperature);
        points.put("battery_current_level", batteryCurrentLevel);
        UpUpdatePoints jmesPath = UpUpdatePoints.newUpUpdatePointsBuilder().points(points).build();
        operations.add(jmesPath);
        // When
        Optional<UpMessage> outputMessage = oprServ.applyUpOperations(inputUpMessage, operations);
        // Then
        Map<String, Point> expectedPoints = new HashMap<>();
        Record expectedTemperatureRecord1 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-02-06T09:14:05.688Z"))
                        .value(jsonMapper.toJsonNode(11.0))
                        .build();
        Record expectedTemperatureRecord2 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-02-06T09:15:45.688Z"))
                        .value(jsonMapper.toJsonNode(11.0))
                        .build();
        Record expectedTemperatureRecord3 =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-02-06T09:17:25.688Z"))
                        .value(jsonMapper.toJsonNode(11.0))
                        .build();
        Record expectedBatteryCurrentLevelRecord =
                Record.newRecordBuilder()
                        .eventTime(OffsetDateTime.parse("2020-02-06T09:18:15.688Z"))
                        .value(jsonMapper.toJsonNode(30.0))
                        .build();
        List<Record> expectedRecords = new ArrayList<>();
        expectedRecords.add(expectedTemperatureRecord1);
        expectedRecords.add(expectedTemperatureRecord2);
        expectedRecords.add(expectedTemperatureRecord3);
        Point expectedTemperature =
                Point.newPointBuilder()
                        .unitId("Far")
                        .type(PointType.INT64)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedRecords.clear();
        expectedRecords.add(expectedBatteryCurrentLevelRecord);
        Point expectedBatteryCurrentLevel =
                Point.newPointBuilder()
                        .unitId("%RH")
                        .type(PointType.INT64)
                        .records(new ArrayList<>(expectedRecords))
                        .build();
        expectedPoints.put("temperature", expectedTemperature);
        expectedPoints.put("battery_current_level", expectedBatteryCurrentLevel);
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).points(expectedPoints).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }
    @Test
    public void should_update_commands() throws PointExtractionException, IOException {
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
        Optional<DownMessage> outputMessage = oprServ.applyDownOperations(inputDownMessage, Collections.singletonList(jmespath));
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("update_command_id_input_jmespath_expected.json"));
        DownMessage expectedOutputMessage = DownMessage.newDownMessageBuilder(inputDownMessage).command(Command.newCommandBuilder().id(expectedMessage.get("id").asText()).input(expectedMessage.get("input")).build()).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

}