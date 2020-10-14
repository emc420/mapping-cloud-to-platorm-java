package com.actility.m2m.ontology.mapper.server;

import com.actility.m2m.commons.http.server.HttpServer;
import com.actility.m2m.commons.http.server.controller.SwaggerController;
import com.actility.m2m.commons.http.server.handler.ServerErrorHandler;
import com.actility.m2m.commons.service.Application;
import com.actility.m2m.ontology.mapper.server.component.DaggerServerComponent;
import com.actility.m2m.ontology.mapper.server.controller.OperationController;
import com.google.common.util.concurrent.ServiceManager;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;

import static com.actility.m2m.commons.http.server.OpenAPI3Utils.createOpenAPI3RouterFactory;

public class Server extends Application {
    @Nonnull
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    @Nonnull
    private final HttpServer httpServer;

    @Inject
    public Server(
            @Nonnull ServiceManager serviceManager,
            @Nonnull Vertx vertx,
            @Nonnull HttpServer httpServer,
            @Nonnull SwaggerController swaggerController,
            @Nonnull OperationController mapperController,
            @Nonnull ServerErrorHandler serverErrorHandler) {
        super(serviceManager);

        this.httpServer = httpServer;

        OpenAPI3RouterFactory routerFactory =
                createOpenAPI3RouterFactory(vertx, "swagger.yaml.vm", serverErrorHandler).blockingGet();

        mapperController.register(routerFactory);
        Router subRouter = routerFactory.getRouter();
        swaggerController.register(subRouter);
        this.httpServer.mountSubRouter("/", subRouter);

        this.httpServer.registerFailureHandler("/*", serverErrorHandler);
    }

    public static void main(String[] args) {
        Server server = DaggerServerComponent.create().providesServer();
        LOG.info("Starting server");
        server.startAsync().awaitRunning();
    }
}
