package com.actility.m2m.ontology.mapper.server;

import com.actility.crafty.core.junit.CraftyTest;
import com.actility.crafty.http.client.HttpClientResource;
import com.actility.crafty.http.server.junit.CraftyHttpClient;
import com.actility.m2m.commons.service.CommonApplication;
import com.actility.m2m.ontology.mapper.server.component.DaggerServerComponent;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.CustomComparator;

import static com.actility.crafty.core.Scenario.scenario;
import static com.actility.crafty.core.builders.MustacheTemplateBuilder.template;
import static com.actility.crafty.core.consumers.ContextMatcherFactory.checkJson;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

@CraftyTest
@CommonApplication(daggerClass = DaggerServerComponent.class, serverClass = Server.class)
public class OntologyMappingAPITest {

    private static final String ontologyMappingBaseUri = "http://localhost:8080";
    @CraftyHttpClient
    private HttpClientResource httpClientResource;

    @Test
    void should_return_elsys_message() throws InterruptedException {
        scenario()
                .send(
                        httpClientResource
                                .post()
                                .header(CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                                .uri(ontologyMappingBaseUri + "/up-apply")
                                .body(context -> template("requests/elsys_request.json").build(context).getBytes()))
                .recv(
                        httpClientResource
                                .statusCode(OK.code())
                                .textBody(
                                        (context, textBody) -> {
                                            checkJson(
                                                    template("response/elsys_response.json"), new CustomComparator(JSONCompareMode.STRICT));
                                        }))
                .start();
    }

    @Test
    void should_return_sensing_labs_message() throws InterruptedException {
        scenario()
                .send(
                        httpClientResource
                                .post()
                                .header(CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                                .uri(ontologyMappingBaseUri + "/up-apply")
                                .body(context -> template("requests/sensing_labs_request.json").build(context).getBytes()))
                .recv(
                        httpClientResource
                                .statusCode(OK.code())
                                .textBody(
                                        (context, textBody) -> {
                                            checkJson(
                                                    template("response/sensing_labs_response.json"),
                                                    new CustomComparator(JSONCompareMode.STRICT));
                                        }))
                .start();
    }

    @Test
    void should_return_nke_message() throws InterruptedException {
        scenario()
                .send(
                        httpClientResource
                                .post()
                                .header(CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                                .uri(ontologyMappingBaseUri + "/up-apply")
                                .body(context -> template("requests/nke_request.json").build(context).getBytes()))
                .recv(
                        httpClientResource
                                .statusCode(OK.code())
                                .textBody(
                                        (context, textBody) -> {
                                            checkJson(
                                                    template("response/nke_response.json"), new CustomComparator(JSONCompareMode.STRICT));
                                        }))
                .start();
    }

    @Test
    void should_fail_when_operations_is_not_array() throws InterruptedException {
        scenario()
                .send(
                        httpClientResource
                                .post()
                                .header(CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                                .uri(ontologyMappingBaseUri + "/up-apply")
                                .body(context -> template("requests/bad_request_operations.json").build(context).getBytes()))
                .recv(httpClientResource.statusCode(BAD_REQUEST.code()))
                .start();
    }

    @Test
    void should_fail_when_up_message_not_found() throws InterruptedException {
        scenario()
                .send(
                        httpClientResource
                                .post()
                                .header(CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                                .uri(ontologyMappingBaseUri + "/up-apply")
                                .body(context -> template("requests/bad_request_up_message.json").build(context).getBytes()))
                .recv(httpClientResource.statusCode(BAD_REQUEST.code()))
                .start();
    }

    @Test
    void should_return_abeeway_message() throws InterruptedException {
        scenario()
                .send(
                        httpClientResource
                                .post()
                                .header(CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                                .uri(ontologyMappingBaseUri + "/up-apply")
                                .body(context -> template("requests/abeeway_request.json").build(context).getBytes()))
                .recv(
                        httpClientResource
                                .statusCode(OK.code())
                                .textBody(
                                        (context, textBody) -> {
                                            checkJson(
                                                    template("response/abeeway_response.json"),
                                                    new CustomComparator(JSONCompareMode.STRICT));
                                        }))
                .start();
    }
    @Test
    void should_return_adeunis_message() throws InterruptedException {
        scenario()
                .send(
                        httpClientResource
                                .post()
                                .header(CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                                .uri(ontologyMappingBaseUri + "/down-apply")
                                .body(context -> template("requests/adeunis_request.json").build(context).getBytes()))
                .recv(
                        httpClientResource
                                .statusCode(OK.code())
                                .textBody(
                                        (context, textBody) -> {
                                            checkJson(
                                                    template("response/adeunis_response.json"),
                                                    new CustomComparator(JSONCompareMode.STRICT));
                                        }))
                .start();
    }
    @Test
    void should_return_theoritical_message() throws InterruptedException {
        scenario()
                .send(
                        httpClientResource
                                .post()
                                .header(CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                                .uri(ontologyMappingBaseUri + "/down-apply")
                                .body(context -> template("requests/theoritical_request.json").build(context).getBytes()))
                .recv(
                        httpClientResource
                                .statusCode(OK.code())
                                .textBody(
                                        (context, textBody) -> {
                                            checkJson(
                                                    template("response/theoritical_response.json"),
                                                    new CustomComparator(JSONCompareMode.STRICT));
                                        }))
                .start();
    }
    @Test
    void should_return_message_as_per_filter_operation() throws InterruptedException {
        scenario()
                .send(
                        httpClientResource
                                .post()
                                .header(CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                                .uri(ontologyMappingBaseUri + "/up-apply")
                                .body(context -> template("requests/filter_request_message.json").build(context).getBytes()))
                .recv(
                        httpClientResource
                                .statusCode(OK.code())
                                .textBody(
                                        (context, textBody) -> {
                                            checkJson(
                                                    template("response/filter_response_message.json"),
                                                    new CustomComparator(JSONCompareMode.STRICT));
                                        }))
                .start();
    }
    @Test
    void should_return_empty_as_per_filter_operation() throws InterruptedException {
        scenario()
                .send(
                        httpClientResource
                                .post()
                                .header(CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                                .uri(ontologyMappingBaseUri + "/up-apply")
                                .body(context -> template("requests/filter_request_empty_message.json").build(context).getBytes()))
                .recv(
                        httpClientResource
                                .statusCode(OK.code())
                                .textBody(
                                        (context, textBody) -> {
                                            checkJson(
                                                    template("response/filter_response_empty_message.json"),
                                                    new CustomComparator(JSONCompareMode.STRICT));
                                        }))
                .start();
    }
    @Test
    void should_return_message_after_points_filter_operation() throws InterruptedException {
        scenario()
                .send(
                        httpClientResource
                                .post()
                                .header(CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                                .uri(ontologyMappingBaseUri + "/up-apply")
                                .body(context -> template("requests/filter_points_request.json").build(context).getBytes()))
                .recv(
                        httpClientResource
                                .statusCode(OK.code())
                                .textBody(
                                        (context, textBody) -> {
                                            checkJson(
                                                    template("response/filter_points_response.json"),
                                                    new CustomComparator(JSONCompareMode.STRICT));
                                        }))
                .start();
    }
    @Test
    void should_return_message_after_update_points_operation() throws InterruptedException {
        scenario()
                .send(
                        httpClientResource
                                .post()
                                .header(CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                                .uri(ontologyMappingBaseUri + "/up-apply")
                                .body(context -> template("requests/update_points_request.json").build(context).getBytes()))
                .recv(
                        httpClientResource
                                .statusCode(OK.code())
                                .textBody(
                                        (context, textBody) -> {
                                            checkJson(
                                                    template("response/update_points_response.json"),
                                                    new CustomComparator(JSONCompareMode.STRICT));
                                        }))
                .start();
    }
    @Test
    void should_return_message_after_update_commands_operation() throws InterruptedException {
        scenario()
                .send(
                        httpClientResource
                                .post()
                                .header(CONTENT_TYPE.toString(), APPLICATION_JSON.toString())
                                .uri(ontologyMappingBaseUri + "/down-apply")
                                .body(context -> template("requests/update_commands_request.json").build(context).getBytes()))
                .recv(
                        httpClientResource
                                .statusCode(OK.code())
                                .textBody(
                                        (context, textBody) -> {
                                            checkJson(
                                                    template("response/update_commands_response.json"),
                                                    new CustomComparator(JSONCompareMode.STRICT));
                                        }))
                .start();
    }
}
