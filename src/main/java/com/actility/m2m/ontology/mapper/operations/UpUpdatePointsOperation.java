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
import com.actility.m2m.ontology.mapping.java.lib.data.UpdatePoint;
import com.actility.m2m.ontology.mapping.java.lib.data.UpOperation;
import com.actility.m2m.ontology.mapping.java.lib.data.UpUpdatePoints;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.*;

public class UpUpdatePointsOperation implements OperationHandler {

    private static final JsonMapper jsonMapper = new JsonMapper(ObjectMapperModule.createObjectMapper());

    @Override
    public Optional<UpMessage> applyUpOperation(UpMessage message, UpOperation upOperation) {
        Map<String, Point> existingPoints =
                new HashMap<>(Optional.ofNullable(message.points).orElse(Collections.emptyMap()));
        Map<String, Point> newPoints = new HashMap<>();
        UpUpdatePoints jmesPathOperation = (UpUpdatePoints) upOperation;
        for (Map.Entry<String, Point> entry : existingPoints.entrySet()) {
            Map<String, UpdatePoint> jmesPathPoints = jmesPathOperation.points;
            if (jmesPathPoints.containsKey(entry.getKey())) {
                boolean isValue = false;
                boolean isAltitude = false;
                boolean isCoordinate = false;
                List<Record> records = entry.getValue().records;
                List<JsonNode> value = new ArrayList<>();
                List<JsonNode> latitudes = new ArrayList<>();
                List<JsonNode> longitudes = new ArrayList<>();
                List<JsonNode> altitudes = new ArrayList<>();
                List<JsonNode> eventTimes = new ArrayList<>();
                for (Record record : records) {
                    if (record.value != null) {
                        value.add(getValue(record.value, jmesPathPoints.get(entry.getKey()).value));
                        isValue = true;
                    }
                    eventTimes.add(getEventTime(jsonMapper.toJsonNode(record.eventTime), jmesPathPoints.get(entry.getKey()).eventTime));
                    if (record.coordinates != null && !record.coordinates.isEmpty()) {
                        isCoordinate = true;
                        if (jmesPathPoints.get(entry.getKey()).coordinates != null) {
                            List<String> getCoordinates = Arrays.asList(jsonMapper.fromJson(jmesPathPoints.get(entry.getKey()).coordinates, String[].class));
                            if (getCoordinates.size() == 2) {
                                longitudes.add(JmesPathUtil.retrieveValues(jsonMapper.toJsonNode(record.coordinates.get(0)), getCoordinates.get(0)));
                                latitudes.add(JmesPathUtil.retrieveValues(jsonMapper.toJsonNode(record.coordinates.get(1)), getCoordinates.get(1)));
                            } else if (getCoordinates.size() == 3) {
                                longitudes.add(JmesPathUtil.retrieveValues(jsonMapper.toJsonNode(record.coordinates.get(0)), getCoordinates.get(0)));
                                latitudes.add(JmesPathUtil.retrieveValues(jsonMapper.toJsonNode(record.coordinates.get(1)), getCoordinates.get(1)));
                                altitudes.add(JmesPathUtil.retrieveValues(jsonMapper.toJsonNode(record.coordinates.get(2)), getCoordinates.get(2)));
                                isAltitude = true;
                            } else {
                                throw new PointExtractionException("invalid 'coordinate' length, it must be 2 or 3", entry.getKey());
                            }
                        } else {
                            longitudes.add(jsonMapper.toJsonNode(record.coordinates.get(0)));
                            latitudes.add(jsonMapper.toJsonNode(record.coordinates.get(1)));
                            if (record.coordinates.size() == 3) {
                                altitudes.add(jsonMapper.toJsonNode(record.coordinates.get(2)));
                                isAltitude = true;
                            }
                        }
                    }
                }
                PointParams params = PointParams.newPointParamsBuilder()
                        .eventTime(jsonMapper.toJsonNode(eventTimes))
                        .values(jsonMapper.toJsonNode(value))
                        .longitude(jsonMapper.toJsonNode(longitudes))
                        .latitude(jsonMapper.toJsonNode(latitudes))
                        .altitude(jsonMapper.toJsonNode(altitudes))
                        .isAltitude(isAltitude)
                        .isCoordinate(isCoordinate)
                        .isValue(isValue)
                        .build();
                Optional<List<Record>> newRecords = JmesPathUtil.extractRecords(params, entry.getKey());
                newRecords.ifPresent(recordList -> newPoints.put(
                        entry.getKey(),
                        Point.newPointBuilder()
                                .ontologyId(Optional.ofNullable(jmesPathPoints.get(entry.getKey()).ontologyId).orElse(entry.getValue().ontologyId))
                                .unitId(Optional.ofNullable(jmesPathPoints.get(entry.getKey()).unitId).orElse(entry.getValue().unitId))
                                .type(
                                        Optional.ofNullable(jmesPathPoints.get(entry.getKey()).type)
                                                .map(type -> PointType.fromValue(type.getValue()))
                                                .orElse(entry.getValue().type))
                                .records(recordList)
                                .build()));
            }else{
                newPoints.put(entry.getKey(), entry.getValue());
            }
        }
        return Optional.of(UpMessage.newUpMessageBuilder(message).points(newPoints).build());
    }

    private JsonNode getValue(JsonNode recordVal, String jmesPathValue){
        if (jmesPathValue != null && jmesPathValue.contains("{{") && jmesPathValue.contains("}}")) {
            return JmesPathUtil.retrieveValues(jsonMapper.toJsonNode(recordVal), jmesPathValue);
        } else if (jmesPathValue != null) {
            return jsonMapper.toJsonNode(jmesPathValue);
        } else {
            return jsonMapper.toJsonNode(recordVal);
        }
    }
    private JsonNode getEventTime(JsonNode recordTime, String jmesPathEventTime){
        if (jmesPathEventTime != null && jmesPathEventTime.contains("{{") && jmesPathEventTime.contains("}}")) {
            return JmesPathUtil.retrieveValues(recordTime, jmesPathEventTime);
        } else if (jmesPathEventTime != null) {
            return jsonMapper.toJsonNode(Objects.requireNonNull(jmesPathEventTime));
        } else {
            return recordTime;
        }
    }

    @Override
    public Optional<DownMessage> applyDownOperation(DownMessage message, DownOperation downOperation) {
        return Optional.empty();
    }


}
