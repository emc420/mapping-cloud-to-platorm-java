package com.actility.m2m.ontology.mapper;

import com.actility.m2m.flow.data.*;
import com.actility.m2m.ontology.mapping.java.lib.data.*;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;

public class OperationServiceTest {

    OperationService oprServ;
    OperationFactory operationFactory;
    OperationHandler operationHandler;

    @BeforeEach
    public void setup() {
        operationFactory = Mockito.mock(OperationFactory.class);
        operationHandler = Mockito.mock(OperationHandler.class);
        oprServ = new OperationService(operationFactory);
    }

    private UpMessage buildInputUpMessage(Map<String, com.actility.m2m.flow.data.Point> inputPoints) {
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
    private DownMessage buildInputDownMessage() throws IOException {
        return DownMessage.newDownMessageBuilder()
                .id("00000000-000000-00000-000000000")
                .time(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                .subAccount(Account.newAccountBuilder().id("subAccount1").realmId("subRealm1").build())
                .origin(
                        DownOrigin.newDownOriginBuilder().id("tpw").type(DownOriginType.PROCESSOR).time(OffsetDateTime.now()).build())
                .content(JsonNodeFactory.instance.objectNode())
                .type(DownMessageType.DEVICEDOWNLINK)
                .command(Command.newCommandBuilder().id("test").build())
                .thing(Thing.newThingBuilder().key("lora:0102030405060708").build())
                .subscriber(
                        Subscriber.newSubscriberBuilder().id("sub1").realmId("realm1").build())
                .build();
    }
    private UpMessage buildInputUpFilterMessage() {

        return UpMessage.newUpMessageBuilder()
                .id("00000000-000000-00000-000000000")
                .time(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                .subAccount(Account.newAccountBuilder().id("subAccount1").realmId("subRealm1").build())
                .origin(
                        UpOrigin.newUpOriginBuilder().id("tpw").type(UpOriginType.BINDER).time(OffsetDateTime.now()).build())
                .content(JsonNodeFactory.instance.objectNode())
                .type(UpMessageType.DEVICEUPLINK)
                .thing(com.actility.m2m.flow.data.Thing.newThingBuilder().key("lora:0102030405060708").build())
                .subscriber(
                        com.actility.m2m.flow.data.Subscriber.newSubscriberBuilder().id("sub1").realmId("realm1").build())
                .build();
    }
    @Test
    public void should_call_jmespath_operation_apply_operation() throws PointExtractionException {
        // Given
        Optional<UpMessage> message1 = Optional.of(buildInputUpMessage(new HashMap<>()));
        Map<String, JmesPathPoint> points = new HashMap<>();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(points).build();

        Mockito.when(operationFactory.build(jmesPath)).thenReturn(operationHandler);
        Mockito.when(operationHandler.applyUpOperation(message1.get(), jmesPath)).thenReturn(message1);
        // When
        message1 = oprServ.applyUpOperations(message1.get(), Collections.singletonList(jmesPath));
        // Then
        Mockito.verify(operationHandler).applyUpOperation(message1.get(), jmesPath);
    }

    @Test
    public void should_call_jmespath_operation_apply_operation_number_of_times_as_list_size()
            throws PointExtractionException {
        // Given
        Point point =
                com.actility.m2m.flow.data.Point.newPointBuilder()
                        .unitId("D")
                        .type(PointType.STRING)
                        .records(new ArrayList<>())
                        .build();
        Map<String, Point> points1 = ImmutableMap.of();
        Map<String, Point> points2 = ImmutableMap.of("dummy1", point);
        Map<String, Point> points3 = ImmutableMap.of("dummy1", point, "dummy2", point);

        Optional<UpMessage> message1 = Optional.of(buildInputUpMessage(points1));
        Optional<UpMessage> message2 = Optional.of(UpMessage.newUpMessageBuilder(message1.get()).points(points2).build());
        Optional<UpMessage> message3 = Optional.of(UpMessage.newUpMessageBuilder(message1.get()).points(points3).build());
        Map<String, JmesPathPoint> points = new HashMap<>();
        UpExtractPoints jmesPath = UpExtractPoints.newUpExtractPointsBuilder().points(points).build();
        Mockito.when(operationFactory.build(jmesPath)).thenReturn(operationHandler);
        Mockito.when(operationHandler.applyUpOperation(message1.get(), jmesPath)).thenReturn(message2);
        Mockito.when(operationHandler.applyUpOperation(message2.get(), jmesPath)).thenReturn(message3);

        // When
        Optional<UpMessage> outputMessage1 = oprServ.applyUpOperations(message1.get(), Collections.nCopies(2, jmesPath));

        // Then
        Mockito.verify(operationFactory, times(2)).build(jmesPath);
        Mockito.verify(operationHandler, times(1)).applyUpOperation(message1.get(), jmesPath);
        Mockito.verify(operationHandler, times(1)).applyUpOperation(message2.get(), jmesPath);
        assertThat(outputMessage1).isEqualTo(message3);
    }
    @Test
    public void should_call_filter_operation_apply_operation() throws PointExtractionException {
        // Given
        Optional<UpMessage> message1 = Optional.of(buildInputUpFilterMessage());
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder().build();

        Mockito.when(operationFactory.build(upFilterOperation)).thenReturn(operationHandler);
        Mockito.when(operationHandler.applyUpOperation(message1.get(), upFilterOperation)).thenReturn(message1);
        // When
        message1 = oprServ.applyUpOperations(message1.get(), Collections.singletonList(upFilterOperation));
        // Then
        Mockito.verify(operationHandler).applyUpOperation(message1.get(), upFilterOperation);
    }

    @Test
    public void should_call_filter_operation_apply_operation_number_of_times_as_list_size()
            throws PointExtractionException {
        // Given

        Optional<UpMessage> message1 = Optional.of(buildInputUpFilterMessage());
        Optional<UpMessage> message2 = Optional.of(UpMessage.newUpMessageBuilder(message1.get()).subType("subType1").build());
        Optional<UpMessage> message3 = Optional.of(UpMessage.newUpMessageBuilder(message1.get()).subType("subType2").build());
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder().keepDeviceUplink(true).build();
        Mockito.when(operationFactory.build(upFilterOperation)).thenReturn(operationHandler);
        Mockito.when(operationHandler.applyUpOperation(message1.get(), upFilterOperation)).thenReturn(message2);
        Mockito.when(operationHandler.applyUpOperation(message2.get(), upFilterOperation)).thenReturn(message3);

        // When
        Optional<UpMessage> outputMessage1 = oprServ.applyUpOperations(message1.get(), Collections.nCopies(2, upFilterOperation));

        // Then
        Mockito.verify(operationFactory, times(2)).build(upFilterOperation);
        Mockito.verify(operationHandler, times(1)).applyUpOperation(message1.get(), upFilterOperation);
        Mockito.verify(operationHandler, times(1)).applyUpOperation(message2.get(), upFilterOperation);
        assertThat(outputMessage1).isEqualTo(message3);
    }
    @Test
    public void should_call_filter_points_operation_apply_operation() throws PointExtractionException {
        // Given
        Optional<UpMessage> message1 = Optional.of(buildInputUpMessage(new HashMap<>()));
        UpFilterPointsOperation upFilterPointsOperation = UpFilterPointsOperation.newUpFilterPointsOperationBuilder().points(new ArrayList<>()).build();

        Mockito.when(operationFactory.build(upFilterPointsOperation)).thenReturn(operationHandler);
        Mockito.when(operationHandler.applyUpOperation(message1.get(), upFilterPointsOperation)).thenReturn(message1);
        // When
        message1 = oprServ.applyUpOperations(message1.get(), Collections.singletonList(upFilterPointsOperation));
        // Then
        Mockito.verify(operationHandler).applyUpOperation(message1.get(), upFilterPointsOperation);
    }
    @Test
    public void should_call_points_filter_operation_apply_operation_number_of_times_as_list_size()
            throws PointExtractionException {
        // Given
        Point point =
                com.actility.m2m.flow.data.Point.newPointBuilder()
                        .unitId("D")
                        .type(PointType.STRING)
                        .records(new ArrayList<>())
                        .build();
        Map<String, Point> points1 = ImmutableMap.of();
        Map<String, Point> points2 = ImmutableMap.of("dummy1", point);
        Map<String, Point> points3 = ImmutableMap.of("dummy1", point, "dummy2", point);

        Optional<UpMessage> message1 = Optional.of(buildInputUpMessage(points1));
        Optional<UpMessage> message2 = Optional.of(UpMessage.newUpMessageBuilder(message1.get()).points(points2).build());
        Optional<UpMessage> message3 = Optional.of(UpMessage.newUpMessageBuilder(message1.get()).points(points3).build());
        UpFilterPointsOperation upFilterPointsOperation = UpFilterPointsOperation.newUpFilterPointsOperationBuilder().points(new ArrayList<>()).build();
        Mockito.when(operationFactory.build(upFilterPointsOperation)).thenReturn(operationHandler);
        Mockito.when(operationHandler.applyUpOperation(message1.get(), upFilterPointsOperation)).thenReturn(message2);
        Mockito.when(operationHandler.applyUpOperation(message2.get(), upFilterPointsOperation)).thenReturn(message3);

        // When
        Optional<UpMessage> outputMessage1 = oprServ.applyUpOperations(message1.get(), Collections.nCopies(2, upFilterPointsOperation));

        // Then
        Mockito.verify(operationFactory, times(2)).build(upFilterPointsOperation);
        Mockito.verify(operationHandler, times(1)).applyUpOperation(message1.get(), upFilterPointsOperation);
        Mockito.verify(operationHandler, times(1)).applyUpOperation(message2.get(), upFilterPointsOperation);
        assertThat(outputMessage1).isEqualTo(message3);
    }

    @Test
    public void should_call_jmespath_update_point_apply_operation() throws PointExtractionException {
        // Given
        Optional<UpMessage> message1 = Optional.of(buildInputUpMessage(new HashMap<>()));
        Map<String, UpdatePoint> points = new HashMap<>();
        UpUpdatePoints jmesPath = UpUpdatePoints.newUpUpdatePointsBuilder().points(points).build();

        Mockito.when(operationFactory.build(jmesPath)).thenReturn(operationHandler);
        Mockito.when(operationHandler.applyUpOperation(message1.get(), jmesPath)).thenReturn(message1);
        // When
        message1 = oprServ.applyUpOperations(message1.get(), Collections.singletonList(jmesPath));
        // Then
        Mockito.verify(operationHandler).applyUpOperation(message1.get(), jmesPath);
    }
    @Test
    public void should_call_jmespath_update_commands_apply_operation() throws IOException {
        // Given
        Optional<DownMessage> message1 = Optional.of(buildInputDownMessage());
        DownUpdateCommand jmesPath = DownUpdateCommand.newDownUpdateCommandBuilder().commands(new HashMap<>()).build();

        Mockito.when(operationFactory.build(jmesPath)).thenReturn(operationHandler);
        Mockito.when(operationHandler.applyDownOperation(message1.get(), jmesPath)).thenReturn(message1);
        // When
        message1 = oprServ.applyDownOperations(message1.get(), Collections.singletonList(jmesPath));
        // Then
        Mockito.verify(operationHandler).applyDownOperation(message1.get(), jmesPath);
    }
}