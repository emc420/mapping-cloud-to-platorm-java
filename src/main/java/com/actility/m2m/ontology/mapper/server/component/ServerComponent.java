package com.actility.m2m.ontology.mapper.server.component;

import com.actility.m2m.commons.http.server.module.HttpServerModule;
import com.actility.m2m.commons.service.mapper.ObjectMapperModule;
import com.actility.m2m.commons.service.module.ServiceManagerModule;
import com.actility.m2m.commons.vertx.module.VertxStandaloneModule;
import com.actility.m2m.ontology.mapper.server.Server;
import com.actility.m2m.ontology.mapper.server.module.SwaggerControllerModule;
import dagger.Component;

import javax.inject.Singleton;

@Singleton
@Component(
        modules = {
                VertxStandaloneModule.class,
                HttpServerModule.class,
                SwaggerControllerModule.class,
                ObjectMapperModule.class,
                ServiceManagerModule.class
        })
public interface ServerComponent {
    Server providesServer();
}
