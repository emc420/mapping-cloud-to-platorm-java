package com.actility.m2m.ontology.mapper.server.module;

import com.actility.m2m.commons.http.server.controller.SwaggerController;
import com.actility.m2m.commons.http.server.handler.VelocityHandler;
import com.actility.m2m.commons.velocity.module.VelocityEngineModule;
import dagger.Module;
import dagger.Provides;

import javax.annotation.Nonnull;

@Module(includes = {VelocityEngineModule.class})
public class SwaggerControllerModule {

    @Provides
    @Nonnull
    public SwaggerController provideSwaggerController(@Nonnull VelocityHandler swaggerHandler) {
        swaggerHandler.template("swagger.yaml.vm");
        return new SwaggerController("/swagger.yaml", swaggerHandler);
    }
}
