package com.actility.m2m.ontology.mapper.jmespath;

import com.actility.m2m.commons.service.mapper.JsonMapper;
import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.actility.m2m.flow.data.Record;
import com.actility.m2m.ontology.mapper.MessageExtractionException;
import com.actility.m2m.ontology.mapper.PointExtractionException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.RuntimeConfiguration;
import io.burt.jmespath.function.FunctionRegistry;
import io.burt.jmespath.jackson.JacksonRuntime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JmesPathUtil {

    public static JmesPath<JsonNode> jmespath;
    private static final JsonMapper jsonMapper = new JsonMapper(ObjectMapperModule.createObjectMapper());

    public static void getJmesPathObject() {
        if (jmespath == null) {
            FunctionRegistry defaultFunctions = FunctionRegistry.defaultRegistry();
            FunctionRegistry customFunctions = defaultFunctions.extend(new ToBooleanFunction(), new DateTimeOpFunction(), new AddPropertyFunction());
            RuntimeConfiguration configuration =
                    new RuntimeConfiguration.Builder().withFunctionRegistry(customFunctions).build();
            jmespath = new JacksonRuntime(configuration);
        }
    }
    @Nullable
    public static JsonNode retrieveValues(@Nonnull JsonNode message, @Nullable String jmesPath) {
        getJmesPathObject();
        if (jmesPath == null) {
            return null;
        }
        if (jmesPath.contains("{{") && jmesPath.contains("}}")) {
            String str = jmesPath.replace("{{", "").replace("}}", "");
            Expression<JsonNode> expression = jmespath.compile(str.replace("\"", ""));
            JsonNode searchResult = expression.search(message);
            if (!searchResult.isNull()) {
                return searchResult;
            }
        }
        return null;
    }

    @Nonnull
    public static Optional<List<Record>> extractRecords(
            PointParams pointParams, String point) {
        List<JsonNode> value = toArray(pointParams.values);
        List<JsonNode> lng = toArray(pointParams.longitude);
        List<JsonNode> lat = toArray(pointParams.latitude);
        List<JsonNode> alt = toArray(pointParams.altitude);
        List<JsonNode> eventTime = toArray(pointParams.eventTime);

        checkCardinality(pointParams, value, lng, lat, alt, eventTime, point);
        if (!value.isEmpty() && !lng.isEmpty() && !alt.isEmpty()) {
            return Optional.of(IntStream
                    .range(0, lng.size())
                    .mapToObj(i -> Record.newRecordBuilder()
                            .eventTime(
                                    OffsetDateTime.parse(eventTime.get(i).asText()))
                            .value(jsonMapper.toJsonNode(value.get(i)))
                            .coordinates(Arrays.asList(lng.get(i).asDouble(),
                                    lat.get(i).asDouble(),
                                    alt.get(i).asDouble()))
                            .build())
                    .collect(Collectors.toList()));
        } else if (!value.isEmpty() && !lng.isEmpty()) {
            return Optional.of(IntStream
                    .range(0, lng.size())
                    .mapToObj(i -> Record.newRecordBuilder()
                            .eventTime(
                                    OffsetDateTime.parse(eventTime.get(i).asText()))
                            .value(jsonMapper.toJsonNode(value.get(i)))
                            .coordinates(Arrays.asList(lng.get(i).asDouble(),
                                    lat.get(i).asDouble()))
                            .build())
                    .collect(Collectors.toList()));
        } else if (!lng.isEmpty() && !alt.isEmpty()) {
            return Optional.of(IntStream
                    .range(0, lng.size())
                    .mapToObj(i -> Record.newRecordBuilder()
                            .eventTime(
                                    OffsetDateTime.parse(eventTime.get(i).asText()))
                            .coordinates(Arrays.asList(lng.get(i).asDouble(),
                                    lat.get(i).asDouble(),
                                    alt.get(i).asDouble()))
                            .build())
                    .collect(Collectors.toList()));
        } else if (!lng.isEmpty()) {
            return Optional.of(IntStream
                    .range(0, lng.size())
                    .mapToObj(i -> Record.newRecordBuilder()
                            .eventTime(
                                    OffsetDateTime.parse(eventTime.get(i).asText()))
                            .coordinates(Arrays.asList(lng.get(i).asDouble(),
                                    lat.get(i).asDouble()))
                            .build())
                    .collect(Collectors.toList()));
        } else if (!value.isEmpty()) {
            return Optional.of(IntStream
                    .range(0, value.size())
                    .mapToObj(i -> Record.newRecordBuilder()
                            .eventTime(
                                    OffsetDateTime.parse(eventTime.get(i).asText()))
                            .value(jsonMapper.toJsonNode(value.get(i)))
                            .build())
                    .collect(Collectors.toList()));
        }

        return Optional.empty();
    }

    public static List<JsonNode> toArray(JsonNode point) {
        if (point == null) {
            return Collections.emptyList();
        } else if (point.isArray()) {
            return Arrays.asList(jsonMapper.fromJson(point, JsonNode[].class));
        } else {
            return Collections.singletonList(point);
        }
    }

    public static void checkCardinality(PointParams pointParams, List<JsonNode> value, List<JsonNode> lng, List<JsonNode> lat, List<JsonNode> alt, List<JsonNode> eventTime, String point) {
        if (pointParams.isValue) {
            if (!value.isEmpty() && value.size() != eventTime.size()) {
                throw new PointExtractionException("there is a mismatch in cardinality for 'value' and 'eventTime'", point);
            }
        }
        if (pointParams.isCoordinate) {
            if (!lng.isEmpty() && lng.size() != eventTime.size()) {
                throw new PointExtractionException(
                        "there is a mismatch in cardinality between 'coordinates' and 'eventTime' fields", point);
            } else if (lng.size() != lat.size() || (pointParams.isAltitude && lng.size() != alt.size())) {
                throw new PointExtractionException(
                        "there is a mismatch in cardinality between 'latitude' and 'longitude' and 'altitude' fields", point);
            }
        }
        if (pointParams.isValue && pointParams.isCoordinate) {
            if (value.size() != lng.size()) {
                throw new PointExtractionException("there is a mismatch in cardinality for 'value' and 'coordinates'", point);
            }
        }

    }

    public static JsonNode extractMessage(JsonNode message, JsonNode operation) {
        if (operation.isValueNode() && operation.asText().contains("{{") && operation.asText().contains("}}")) {
            JsonNode retrievedJmesValue = retrieveValues(message, operation.asText());
            if(retrievedJmesValue!=null && retrievedJmesValue.isObject()){
                return retrievedJmesValue;
            }else {
                throw new MessageExtractionException("expected object for 'message' but returned value node or null");  }
        }
        if (operation.isValueNode()) {
            throw new MessageExtractionException("expected object but is a value node");
        }
        return extractMessageRecursion(message, operation);
    }
    public static JsonNode extractCommands(JsonNode message, JsonNode operation) {
        if (operation.isValueNode() && operation.asText().contains("{{") && operation.asText().contains("}}")) {
            JsonNode retrievedJmesValue = retrieveValues(message, operation.asText());
            if(retrievedJmesValue!=null){
                return retrievedJmesValue;
            }else {
                throw new MessageExtractionException("retrieved value is null");  }
        }
        if (operation.isValueNode()) {
            return operation;
        }
        return extractMessageRecursion(message, operation);
    }
    public static JsonNode extractMessageRecursion(JsonNode message, JsonNode operation){
        ObjectNode returnJson = ObjectMapperModule.createObjectMapper().createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> iterator = operation.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            if (!entry.getValue().isValueNode()) {
                returnJson.set(entry.getKey(), extractMessageRecursion(message, entry.getValue()));
            } else if (entry.getValue().asText().contains("{{") && entry.getValue().asText().contains("}}")) {
                JsonNode value = retrieveValues(message, entry.getValue().asText());
                if (value != null) {
                    returnJson.set(entry.getKey(), value);
                } else {
                    throw new MessageExtractionException("nothing could be extracted from the jmes expression, entry.getValue().asText()");
                }
            }else{
                returnJson.set(entry.getKey(), entry.getValue());
            }
        }
        return returnJson;
    }
}
