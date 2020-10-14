package com.actility.m2m.ontology.mapper.operations;

import com.actility.m2m.commons.service.mapper.JsonMapper;
import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.actility.m2m.flow.data.*;
import com.actility.m2m.flow.data.Record;
import com.actility.m2m.ontology.mapper.OperationHandler;
import com.actility.m2m.ontology.mapper.PointExtractionException;
import com.actility.m2m.ontology.mapper.jmespath.JmesPathUtil;
import com.actility.m2m.ontology.mapper.jmespath.PointParams;
import com.actility.m2m.ontology.mapping.java.lib.data.DownOperation;
import com.actility.m2m.ontology.mapping.java.lib.data.JmesPathPoint;
import com.actility.m2m.ontology.mapping.java.lib.data.UpExtractPoints;
import com.actility.m2m.ontology.mapping.java.lib.data.UpOperation;
import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nonnull;
import java.util.*;

public class UpExtractPointsOperation implements OperationHandler {

    private static final JsonMapper jsonMapper = new JsonMapper(ObjectMapperModule.createObjectMapper());

    @Override
    @Nonnull
    public Optional<UpMessage> applyUpOperation(@Nonnull UpMessage message, @Nonnull UpOperation upOperation) {
        Map<String, Point> newPoints =
                new HashMap<>(Optional.ofNullable(message.points).orElse(Collections.emptyMap()));
        UpExtractPoints jmesPathOperation = (UpExtractPoints) upOperation;
        JsonNode messageJson = jsonMapper.toJsonNode(message);
        for (Map.Entry<String, JmesPathPoint> entry : jmesPathOperation.points.entrySet()) {
            JsonNode values = null;
            boolean isValue = false;
            if (entry.getValue().value != null) {
                values = JmesPathUtil.retrieveValues(messageJson, entry.getValue().value);
                isValue = true;
            }
            JsonNode eventTime = JmesPathUtil.retrieveValues(messageJson, entry.getValue().eventTime);
            JsonNode longitude = null;
            JsonNode latitude = null;
            JsonNode altitude = null;
            boolean isAltitude = false;
            boolean isCoordinate = false;
            if (entry.getValue().coordinates != null) {
                List<String> getCoordinates = Arrays.asList(jsonMapper.fromJson(entry.getValue().coordinates, String[].class));
                isCoordinate = true;
                if (getCoordinates.size() == 2) {
                    longitude = JmesPathUtil.retrieveValues(messageJson, getCoordinates.get(0));
                    latitude = JmesPathUtil.retrieveValues(messageJson, getCoordinates.get(1));

                } else if (getCoordinates.size() == 3) {
                    longitude = JmesPathUtil.retrieveValues(messageJson, getCoordinates.get(0));
                    latitude = JmesPathUtil.retrieveValues(messageJson, getCoordinates.get(1));
                    altitude = JmesPathUtil.retrieveValues(messageJson, getCoordinates.get(2));
                    isAltitude = true;
                } else {
                    throw new PointExtractionException("invalid 'coordinate' length, it must be 2 or 3", entry.getKey());
                }
            }
            PointParams params = PointParams.newPointParamsBuilder()
                    .eventTime(eventTime)
                    .values(values)
                    .longitude(longitude)
                    .latitude(latitude)
                    .altitude(altitude)
                    .isAltitude(isAltitude)
                    .isCoordinate(isCoordinate)
                    .isValue(isValue)
                    .build();
            Optional<List<Record>> records = JmesPathUtil.extractRecords(params, entry.getKey());
            records.ifPresent(recordList -> newPoints.put(
                    entry.getKey(),
                    Point.newPointBuilder()
                            .ontologyId(Optional.ofNullable(entry.getValue().ontologyId).orElse(null))
                            .unitId(Optional.ofNullable(entry.getValue().unitId).orElse(null))
                            .type(
                                    Optional.ofNullable(entry.getValue().type)
                                            .map(type -> PointType.fromValue(type.getValue()))
                                            .orElse(null))
                            .records(recordList)
                            .build()));
        }
        return Optional.of(UpMessage.newUpMessageBuilder(message).points(newPoints).build());
    }

    @Override
    public Optional<DownMessage> applyDownOperation(DownMessage message, DownOperation upOperation) {
        return Optional.empty();
    }
}
