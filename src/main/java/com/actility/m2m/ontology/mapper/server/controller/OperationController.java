package com.actility.m2m.ontology.mapper.server.controller;

import com.actility.m2m.commons.http.server.CommonsRoutingContext;
import com.actility.m2m.commons.http.server.controller.OpenAPI3Controller;
import com.actility.m2m.commons.service.mapper.JsonMapper;
import com.actility.m2m.flow.data.DownMessage;
import com.actility.m2m.flow.data.UpMessage;
import com.actility.m2m.ontology.mapper.OperationService;
import com.actility.m2m.ontology.mapping.java.lib.data.DownApplyOperations;
import com.actility.m2m.ontology.mapping.java.lib.data.UpApplyOperations;
import com.google.common.net.MediaType;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class OperationController implements OpenAPI3Controller {
    @Nonnull
    private static final Logger LOG = LoggerFactory.getLogger(OperationController.class);
    @Nonnull
    private OperationService operationService;
    @Nonnull
    private JsonMapper jsonMapper;

    @Inject
    public OperationController(@Nonnull OperationService operationService, @Nonnull JsonMapper jsonMapper) {
        this.operationService = operationService;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public void register(OpenAPI3RouterFactory router) {
        router.addHandlerByOperationId("applyOperations", this::handleApplyOperations);
        router.addHandlerByOperationId("applyOperationsDown", this::handleApplyOperationsDown);
    }

    private void handleApplyOperations(@Nonnull RoutingContext routingContext) {
        CommonsRoutingContext commonsRoutingContext = CommonsRoutingContext.wrap(routingContext);

        commonsRoutingContext
                .readBody()
                .map(buffer -> buffer.toString(StandardCharsets.UTF_8))
                .map(body -> jsonMapper.fromJson(body, UpApplyOperations.class))
                .map(
                        requestJson ->
                                operationService.applyUpOperations(
                                        jsonMapper.fromJson(jsonMapper.toJson(requestJson.message), UpMessage.class),
                                        requestJson.operations))
                .subscribe(
                        upMessage ->
                                commonsRoutingContext
                                        .response()
                                        .setContentType(MediaType.JSON_UTF_8)
                                        .end(jsonMapper.toJson(Optional.ofNullable(upMessage))),
                        routingContext::fail);
    }
    private void handleApplyOperationsDown(@Nonnull RoutingContext routingContext) {
        CommonsRoutingContext commonsRoutingContext = CommonsRoutingContext.wrap(routingContext);

        commonsRoutingContext
                .readBody()
                .map(buffer -> buffer.toString(StandardCharsets.UTF_8))
                .map(body -> jsonMapper.fromJson(body, DownApplyOperations.class))
                .map(
                        requestJson ->
                                operationService.applyDownOperations(
                                        jsonMapper.fromJson(jsonMapper.toJson(requestJson.message), DownMessage.class),
                                        requestJson.operationsDown))
                .subscribe(
                        downMessage ->
                                commonsRoutingContext
                                        .response()
                                        .setContentType(MediaType.JSON_UTF_8)
                                        .end(jsonMapper.toJson(Optional.ofNullable(downMessage))),
                        routingContext::fail);
    }
}