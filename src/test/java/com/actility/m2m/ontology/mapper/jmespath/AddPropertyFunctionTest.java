package com.actility.m2m.ontology.mapper.jmespath;

import com.actility.m2m.commons.service.mapper.JsonMapper;
import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.burt.jmespath.Adapter;
import io.burt.jmespath.function.FunctionArgument;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AddPropertyFunctionTest {

    private static JsonMapper jsonMapper = new JsonMapper(ObjectMapperModule.createObjectMapper());

    @Test
    public void should_extract_required_object_for_add_property_function() throws IOException {
        // Given
        AddPropertyFunction add_property = new AddPropertyFunction();
        ObjectNode inputMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("function_inside_jmespath_expression.json"));
        //When
        List<FunctionArgument<JsonNode>> arguments = new ArrayList<>();
        arguments.add(FunctionArgument.of(inputMessage));
        arguments.add(FunctionArgument.of(jsonMapper.toJsonNode("\"type\"")));
        arguments.add(FunctionArgument.of(jsonMapper.toJsonNode("\"myDeviceCommand\"")));
        JsonNode outputMessage = add_property.callFunction(Mockito.mock(Adapter.class), arguments);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("function_inside_jmespath_expression_expected.json"));
        assertThat(outputMessage).isEqualTo(expectedMessage);
    }

    @Test
    public void should_merge_property_if_already_present_for_add_property_function() throws IOException {
        // Given
        AddPropertyFunction add_property = new AddPropertyFunction();
        ObjectNode inputMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("property_if_already_present_for_add_property.json"));
        //When
        List<FunctionArgument<JsonNode>> arguments = new ArrayList<>();
        arguments.add(FunctionArgument.of(inputMessage));
        arguments.add(FunctionArgument.of(jsonMapper.toJsonNode("\"type\"")));
        arguments.add(FunctionArgument.of(jsonMapper.toJsonNode("\"Thanos was right\"")));
        JsonNode outputMessage = add_property.callFunction(Mockito.mock(Adapter.class), arguments);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("property_if_already_present_for_add_property_expected.json"));
        assertThat(outputMessage).isEqualTo(expectedMessage);
    }

    @Test
    public void should_merge_if_property_is_an_object_for_add_property_function() throws IOException {
        // Given
        AddPropertyFunction add_property = new AddPropertyFunction();
        ObjectNode inputMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("if_property_is_an_object_for_add_property.json"));

        ObjectNode propertyvalue =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("property_value_is_object.json"));
        //When
        List<FunctionArgument<JsonNode>> arguments = new ArrayList<>();
        arguments.add(FunctionArgument.of(inputMessage));
        arguments.add(FunctionArgument.of(jsonMapper.toJsonNode("\"type\"")));
        arguments.add(FunctionArgument.of(jsonMapper.toJsonNode(propertyvalue)));
        JsonNode outputMessage = add_property.callFunction(Mockito.mock(Adapter.class), arguments);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("if_property_is_an_object_for_add_property_expected.json"));
        assertThat(outputMessage).isEqualTo(expectedMessage);
    }
    @Test
    public void should_merge_empty_first_parameter_for_add_property_function() throws IOException {
        // Given
        AddPropertyFunction add_property = new AddPropertyFunction();
        ObjectNode inputMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("empty_first_parameter_for_add_property.json"));
        //When
        List<FunctionArgument<JsonNode>> arguments = new ArrayList<>();
        arguments.add(FunctionArgument.of(inputMessage));
        arguments.add(FunctionArgument.of(jsonMapper.toJsonNode("\"type\"")));
        arguments.add(FunctionArgument.of(jsonMapper.toJsonNode("\"Thanos was right\"")));
        JsonNode outputMessage = add_property.callFunction(Mockito.mock(Adapter.class), arguments);
        //Then
        ObjectNode expectedMessage =
                (ObjectNode)
                        ObjectMapperModule.createObjectMapper()
                                .readTree(getClass().getClassLoader().getResourceAsStream("empty_first_parameter_for_add_property_expected.json"));
        assertThat(outputMessage).isEqualTo(expectedMessage);
    }

}
