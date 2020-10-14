package com.actility.m2m.ontology.mapper.operations;

import com.actility.m2m.flow.data.*;
import com.actility.m2m.ontology.mapper.PointExtractionException;
import com.actility.m2m.ontology.mapping.java.lib.data.UpFilterOperation;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterOperationTest {

    FilterOperation filterOperation = new FilterOperation();

    private UpMessage buildInputUpMessage(UpMessageType type) {

        return UpMessage.newUpMessageBuilder()
                .id("00000000-000000-00000-000000000")
                .time(OffsetDateTime.parse("2020-01-01T10:00:00.000Z"))
                .subAccount(Account.newAccountBuilder().id("subAccount1").realmId("subRealm1").build())
                .origin(
                        UpOrigin.newUpOriginBuilder().id("tpw").type(UpOriginType.BINDER).time(OffsetDateTime.now()).build())
                .content(JsonNodeFactory.instance.objectNode())
                .type(type)
                .thing(com.actility.m2m.flow.data.Thing.newThingBuilder().key("lora:0102030405060708").build())
                .subscriber(
                        com.actility.m2m.flow.data.Subscriber.newSubscriberBuilder().id("sub1").realmId("realm1").build())
                .build();
    }

    @Test
    public void should_include_message_for_keep_device_downlink_sent_true_type_device_downlink_sent() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEDOWNLINKSENT);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceDownlinkSent(true)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_message_for_keep_device_downlink_sent_true_type_not_device_downlink_sent() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEUPLINK);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceDownlinkSent(true)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then

        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_downlink_sent_false_type_device_downlink_sent() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEDOWNLINKSENT);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceDownlinkSent(false)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then

        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_downlink_sent_false_type_not_device_downlink_sent() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEUPLINK);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceDownlinkSent(false)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_include_message_for_keep_device_uplink_true_type_device_uplink() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEUPLINK);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceUplink(true)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_message_for_keep_device_uplink_true_type_not_device_uplink() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEDOWNLINKSENT);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceUplink(true)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_uplink_false_type_device_uplink() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEUPLINK);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceUplink(false)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_uplink_false_type_not_device_uplink() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEDOWNLINKSENT);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceUplink(false)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_include_message_for_keel_device_Location_true_type_device_location() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICELOCATION);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceLocation(true)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_message_for_keep_device_location_true_type_not_device_location() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEDOWNLINKSENT);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceLocation(true)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_location_false_type_device_location() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICELOCATION);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceLocation(false)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_location_false_type_not_device_location() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEDOWNLINKSENT);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceLocation(false)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_notification_false_message_type_device_notification() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICENOTIFICATION);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(false)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_notification_false_message_type_not_device_notification() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEDOWNLINKSENT);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(false)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_include_message_for_keep_device_notification_true_sub_type_null_message_sub_type_null_device_notification_present() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICENOTIFICATION);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_message_for_keep_device_notification_true_sub_type_null_message_sub_type_null_device_notification_absent() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEDOWNLINKSENT);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_include_message_for_keep_device_notification_true_sub_type_null_message_sub_type_not_null_device_notification_present() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = UpMessage.newUpMessageBuilder(buildInputUpMessage(UpMessageType.DEVICENOTIFICATION)).subType("subType1").build();
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_message_for_keep_device_notification_true_sub_type_null_message_sub_type_not_null_device_notification_absent() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = UpMessage.newUpMessageBuilder(buildInputUpMessage(UpMessageType.DEVICEDOWNLINKSENT)).subType("subType1").build();
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_include_message_for_keep_device_notification_true_sub_type_empty_message_sub_type_null_device_notification_present() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICENOTIFICATION);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .keepDeviceNotificationSubTypes(new ArrayList<>())
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();

        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_message_for_keep_device_notification_true_sub_type_empty_message_sub_type_null_device_notification_absent() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEUPLINK);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .keepDeviceNotificationSubTypes(new ArrayList<>())
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_notification_true_sub_type_empty_message_sub_type_not_null_device_notification_absent() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = UpMessage.newUpMessageBuilder(buildInputUpMessage(UpMessageType.DEVICEDOWNLINKSENT)).subType("subType1").build();
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .keepDeviceNotificationSubTypes(new ArrayList<>())
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_notification_true_sub_type_empty_message_sub_type_not_null_device_notification_present() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = UpMessage.newUpMessageBuilder(buildInputUpMessage(UpMessageType.DEVICENOTIFICATION)).subType("subType1").build();
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .keepDeviceNotificationSubTypes(new ArrayList<>())
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_include_message_for_keep_device_notification_true_sub_type_not_empty_message_sub_type_not_null_and_match_device_notification_present() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = UpMessage.newUpMessageBuilder(buildInputUpMessage(UpMessageType.DEVICENOTIFICATION)).subType("subType1").build();
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .keepDeviceNotificationSubTypes(Arrays.asList("subType1", "subType2"))
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        UpMessage expectedOutputMessage = UpMessage.newUpMessageBuilder(inputUpMessage).build();
        assertThat(outputMessage).hasValue(expectedOutputMessage);
    }

    @Test
    public void should_exclude_message_for_keep_deviceNotification_true_sub_type_not_empty_message_sub_type_not_null_and_match_device_notification_absent() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = UpMessage.newUpMessageBuilder(buildInputUpMessage(UpMessageType.DEVICEUPLINK)).subType("subType1").build();
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .keepDeviceNotificationSubTypes(Arrays.asList("subType1", "subType2"))
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_notification_true_sub_type_not_empty_message_sub_type_not_null_and_not_match_device_notification_present() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = UpMessage.newUpMessageBuilder(buildInputUpMessage(UpMessageType.DEVICENOTIFICATION)).subType("subType3").build();
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .keepDeviceNotificationSubTypes(Arrays.asList("subType1", "subType2"))
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_notification_true_sub_type_not_empty_message_sub_type_not_null_and_not_match_device_notification_absent() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = UpMessage.newUpMessageBuilder(buildInputUpMessage(UpMessageType.DEVICEUPLINK)).subType("subType3").build();
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .keepDeviceNotificationSubTypes(Arrays.asList("subType1", "subType2"))
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_notification_true_sub_type_not_empty_message_sub_type_null_device_notification_present() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICENOTIFICATION);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .keepDeviceNotificationSubTypes(Arrays.asList("subType1", "subType2"))
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }

    @Test
    public void should_exclude_message_for_keep_device_notification_true_sub_type_not_empty_message_sub_type_null_device_notification_absent() throws PointExtractionException {
        // Given
        UpMessage inputUpMessage = buildInputUpMessage(UpMessageType.DEVICEUPLINK);
        UpFilterOperation upFilterOperation = UpFilterOperation.newUpFilterOperationBuilder()
                .keepDeviceNotification(true)
                .keepDeviceNotificationSubTypes(Arrays.asList("subType1", "subType2"))
                .build();
        // When
        Optional<UpMessage> outputMessage = filterOperation.applyUpOperation(inputUpMessage, upFilterOperation);
        // Then
        assertThat(outputMessage).isEmpty();
    }
}