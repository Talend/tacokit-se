/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.rest.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.talend.components.rest.configuration.Format;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.configuration.auth.Authentication;
import org.talend.components.rest.configuration.auth.Authorization;
import org.talend.components.rest.configuration.auth.Basic;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
@Testcontainers
@Tag("ITs")
// Identify those tests as integration tests to exclude them since some difficulties to run them on ci currently
@WithComponents(value = "org.talend.components.rest")
public class ClientTestWithHttpbinTest {

    private static GenericContainer<?> httpbin;

    public static Supplier<String> HTTPBIN_BASE;

    private final static int CONNECT_TIMEOUT = 30000;

    private final static int READ_TIMEOUT = 30000;

    @Service
    RestService service;

    @Service
    RecordBuilderService recordBuilderService;

    @Service
    JsonReaderFactory jsonReaderFactory;

    @Injected
    private BaseComponentsHandler handler;

    private RequestConfig config;

    private boolean followRedirects_backup;

    @BeforeAll
    static void startHttpBinContainer() {
        httpbin = new GenericContainer<>("kennethreitz/httpbin").withExposedPorts(80).waitingFor(Wait.forHttp("/"));
        httpbin.start();
        HTTPBIN_BASE = () -> System.getProperty("org.talend.components.rest.httpbin_base",
                "http://localhost:" + httpbin.getMappedPort(80));
    }

    @AfterAll
    static void stopHttpBinContainer() {
        httpbin.stop();
    }

    @BeforeEach
    void before() {
        followRedirects_backup = HttpURLConnection.getFollowRedirects();
        HttpURLConnection.setFollowRedirects(false);

        // Inject needed services
        handler.injectServices(this);

        config = RequestConfigBuilderTest.getEmptyRequestConfig();

        config.getDataset().getDatastore().setBase(HTTPBIN_BASE.get());
        config.getDataset().getDatastore().setConnectionTimeout(CONNECT_TIMEOUT);
        config.getDataset().getDatastore().setReadTimeout(READ_TIMEOUT);
    }

    @AfterEach
    void after() {
        HttpURLConnection.setFollowRedirects(followRedirects_backup);
    }

    @Test
    void httpbinGet() throws MalformedURLException {
        config.getDataset().setResource("get");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setFormat(Format.JSON);

        Iterator<Record> respIt = recordBuilderService.buildFixedRecord(service.execute(config), config);

        final Record resp = respIt.next();

        assertFalse(respIt.hasNext());
        assertEquals(200, resp.getInt("status"));

        final Record body = resp.getRecord("body");
        URL base = new URL(HTTPBIN_BASE.get());
        assertEquals(service.buildUrl(config, Collections.emptyMap()), body.getString("url"));
        assertEquals(base.getHost() + ":" + base.getPort(), body.getRecord("headers").getString("Host"));
    }

    /**
     * If there are some parameters set, if false is given to setHasXxxx those parameters should not be passed.
     *
     * @throws Exception
     */
    @Test
    void testParamsDisabled() throws MalformedURLException {
        HttpMethod[] verbs = { HttpMethod.DELETE, HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT };
        config.getDataset().setResource("get");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setFormat(Format.JSON);

        List<Param> queryParams = new ArrayList<>();
        queryParams.add(new Param("params1", "value1"));
        config.getDataset().setHasQueryParams(false);
        config.getDataset().setQueryParams(queryParams);

        List<Param> headerParams = new ArrayList<>();
        headerParams.add(new Param("Header1", "simple value"));
        config.getDataset().setHasHeaders(false);
        config.getDataset().setHeaders(headerParams);

        Iterator<Record> respIt = recordBuilderService.buildFixedRecord(service.execute(config), config);

        final Record resp = respIt.next();
        assertFalse(respIt.hasNext());
        assertEquals(200, resp.getInt("status"));

        assertFalse(resp.getRecord("body").getRecord("args").getOptionalString("params1").isPresent());

        URL base = new URL(HTTPBIN_BASE.get());
        assertEquals(base.getHost() + ":" + base.getPort(), resp.getRecord("body").getRecord("headers").getString("Host"));
    }

    @Test
    void testQueryAndHeaderParams() {
        HttpMethod[] verbs = { HttpMethod.DELETE, HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT };
        for (HttpMethod m : verbs) {
            config.getDataset().setResource(m.name().toLowerCase());
            config.getDataset().setMethodType(m);
            config.getDataset().setFormat(Format.JSON);

            List<Param> queryParams = new ArrayList<>();
            queryParams.add(new Param("params1", "value1"));
            queryParams.add(new Param("params2", "<name>Dupont & Dupond</name>"));
            config.getDataset().setHasQueryParams(true);
            config.getDataset().setQueryParams(queryParams);

            List<Param> headerParams = new ArrayList<>();
            headerParams.add(new Param("Header1", "simple value"));
            headerParams.add(new Param("Header2", "<name>header Dupont & Dupond</name>"));
            config.getDataset().setHasHeaders(true);
            config.getDataset().setHeaders(headerParams);

            final Iterator<Record> respIt = recordBuilderService.buildFixedRecord(service.execute(config), config);

            final Record resp = respIt.next();
            assertFalse(respIt.hasNext());

            assertEquals(200, resp.getInt("status"));

            assertEquals("value1", resp.getRecord("body").getRecord("args").getString("params1"));
            assertEquals("<name>Dupont & Dupond</name>", resp.getRecord("body").getRecord("args").getString("params2"));
            assertEquals("simple value", resp.getRecord("body").getRecord("headers").getString("Header1"));
            assertEquals("<name>header Dupont & Dupond</name>", resp.getRecord("body").getRecord("headers").getString("Header2"));
        }
    }

    @Test
    void testBasicAuth() {
        String user = "my_user";
        String pwd = "my_password";

        Basic basic = new Basic();
        basic.setUsername(user);
        basic.setPassword(pwd);

        Authentication auth = new Authentication();
        auth.setType(Authorization.AuthorizationType.Basic);
        auth.setBasic(basic);

        config.getDataset().getDatastore().setAuthentication(auth);
        config.getDataset().setMethodType(HttpMethod.GET);

        config.getDataset().setResource("/basic-auth/" + user + "/wrong_" + pwd);
        final Iterator<Record> respForbiddenIt = recordBuilderService.buildFixedRecord(service.execute(config), config);
        final Record respForbidden = respForbiddenIt.next();
        assertFalse(respForbiddenIt.hasNext());
        assertEquals(401, respForbidden.getInt("status"));

        config.getDataset().setResource("/basic-auth/" + user + "/" + pwd);
        final Iterator<Record> respOkIt = recordBuilderService.buildFixedRecord(service.execute(config), config);
        final Record respOk = respOkIt.next();
        assertFalse(respOkIt.hasNext());
        assertEquals(200, respOk.getInt("status"));
    }

    @Test
    void testBearerAuth() {
        Authentication auth = new Authentication();
        auth.setType(Authorization.AuthorizationType.Bearer);

        config.getDataset().getDatastore().setBase(HTTPBIN_BASE.get());
        config.getDataset().getDatastore().setAuthentication(auth);
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("/bearer");

        auth.setBearerToken("");
        final Iterator<Record> respKoIt = recordBuilderService.buildFixedRecord(service.execute(config), config);
        final Record respKo = respKoIt.next();
        assertFalse(respKoIt.hasNext());
        assertEquals(401, respKo.getInt("status"));

        auth.setBearerToken("token-123456789");
        final Iterator<Record> respOkIt = recordBuilderService.buildFixedRecord(service.execute(config), config);
        final Record respOk = respOkIt.next();
        assertFalse(respOkIt.hasNext());
        assertEquals(200, respOk.getInt("status"));
    }

    @ParameterizedTest
    @CsvSource(value = { "GET", "POST", "PUT" })
    void testRedirect(final String method) {

        String redirect_url = HTTPBIN_BASE.get() + "/" + method.toLowerCase() + "?redirect=ok";
        config.getDataset().setResource("redirect-to?url=" + redirect_url);
        config.getDataset().setMethodType(HttpMethod.valueOf(method));
        config.getDataset().setMaxRedirect(1);

        final Iterator<Record> respIt = recordBuilderService.buildFixedRecord(service.execute(config), config);
        final Record resp = respIt.next();
        assertFalse(respIt.hasNext());
        assertEquals(200, resp.getInt("status"));

        // todo: A REVOIR
        /*
         * JsonObject payload = (JsonObject) resp.getBody();
         *
         * assertEquals("ok", payload.getJsonObject("args").getString("redirect"));
         */
    }

    @ParameterizedTest
    @CsvSource(value = { "GET,", "GET,http://www.google.com" })
    void testRedirectOnlySameHost(final String method, final String redirect_url) throws MalformedURLException {
        String mainHost = new URL(HTTPBIN_BASE.get()).getHost();

        config.getDataset().setResource("redirect-to?url=" + ("".equals(redirect_url) ? mainHost : redirect_url));
        config.getDataset().setMethodType(HttpMethod.valueOf(method));
        config.getDataset().setMaxRedirect(1);
        config.getDataset().setOnly_same_host(true);

        if ("".equals(redirect_url)) {
            final Iterator<Record> respIt = recordBuilderService.buildFixedRecord(service.execute(config), config);
            final Record resp = respIt.next();
            assertFalse(respIt.hasNext());
            assertEquals(200, resp.getInt("status"));

            // todo: A REVOIR
            /*
             * JsonObject payload = (JsonObject) resp.getBody();
             *
             * assertEquals("ok", payload.getJsonObject("args").getString("redirect"));
             */
        } else {
            assertThrows(IllegalArgumentException.class,
                    () -> recordBuilderService.buildFixedRecord(service.execute(config), config));
        }
    }

    @ParameterizedTest
    @CsvSource(value = { "6,-1", "3,3", "3,5" })
    void testRedirectNOk(final int nbRedirect, final int maxRedict) {
        config.getDataset().setResource("redirect/" + nbRedirect);
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setMaxRedirect(maxRedict);

        final Iterator<Record> respIt = recordBuilderService.buildFixedRecord(service.execute(config), config);
        final Record resp = respIt.next();
        assertFalse(respIt.hasNext());
        assertEquals(200, resp.getInt("status"));
    }

    @ParameterizedTest
    @CsvSource(value = { "3,0", "3,1", "3,2", "5,4" })
    void testRedirectNko(final int nbRedirect, final int maxRedict) {
        config.getDataset().setResource("redirect/" + nbRedirect);
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setMaxRedirect(maxRedict);

        if (maxRedict == 0) {
            // When maxRedirect == 0 then redirect is disabled
            // we only return the response
            final Iterator<Record> respIt = recordBuilderService.buildFixedRecord(service.execute(config), config);
            final Record resp = respIt.next();
            assertFalse(respIt.hasNext());
            assertEquals(302, resp.getInt("status"));
        } else {
            Exception e = assertThrows(IllegalArgumentException.class,
                    () -> recordBuilderService.buildFixedRecord(service.execute(config), config));
        }
    }

    @ParameterizedTest
    @CsvSource(value = { "auth-int,MD5", "auth,MD5", "auth-int,MD5-sess", "auth,MD5-sess", "auth-int,SHA-256", "auth,SHA-256",
            "auth-int,SHA-512", "auth,SHA-512" })
    void testDisgestAuth(final String qop, final String algo) {
        String user = "my_user";
        String pwd = "my_password";

        Basic basic = new Basic();
        basic.setUsername(user);
        basic.setPassword(pwd);

        Authentication auth = new Authentication();
        auth.setType(Authorization.AuthorizationType.Digest);
        auth.setBasic(basic);

        testDigestAuthWithQop(200, user, pwd, auth, qop);
        testDigestAuthWithQop(401, user, pwd + "x", auth, qop);

        testDigestAuthWithQopAlgo(200, user, pwd, auth, qop, algo);
        testDigestAuthWithQopAlgo(401, user, pwd + "x", auth, qop, algo);
    }

    private void testDigestAuthWithQop(final int expected, final String user, final String pwd, final Authentication auth,
            final String qop) {
        config.getDataset().getDatastore().setAuthentication(auth);
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("digest-auth/" + qop + "/" + user + "/" + pwd);

        final Iterator<Record> respIt = recordBuilderService.buildFixedRecord(service.execute(config), config);
        final Record resp = respIt.next();
        assertFalse(respIt.hasNext());
        assertEquals(expected, resp.getInt("status"));
    }

    private void testDigestAuthWithQopAlgo(final int expected, final String user, final String pwd, final Authentication auth,
            final String qop, final String algo) {
        config.getDataset().getDatastore().setAuthentication(auth);
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("digest-auth/" + qop + "/" + user + "/" + pwd + "/" + algo);

        final Iterator<Record> respIt = recordBuilderService.buildFixedRecord(service.execute(config), config);
        final Record resp = respIt.next();
        assertFalse(respIt.hasNext());
        assertEquals(expected, resp.getInt("status"));
    }

    @ParameterizedTest
    @CsvSource(value = { "json", "json_notparsed", "xml", "html" })
    void testformats(final String type) {
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource(type.endsWith("_notparsed") ? type.substring(0, type.length() - 10) : type);
        if ("json".equals(type)) {
            config.getDataset().setFormat(Format.JSON);
        }

        final Iterator<Record> respIt = recordBuilderService.buildFixedRecord(service.execute(config), config);
        final Record resp = respIt.next();
        assertFalse(respIt.hasNext());
        assertEquals(200, resp.getInt("status"));

        if ("json".equals(type)) {
            assertEquals("Sample Slide Show", resp.getRecord("body").getRecord("slideshow").getString("title"));
        } else if ("json_notparsed".equals(type)) {
            final String body = resp.getString("body");
            final String expected = "{\n" + "  \"slideshow\": {\n" + "    \"author\": \"Yours Truly\", \n"
                    + "    \"date\": \"date of publication\", \n" + "    \"slides\": [\n" + "      {\n"
                    + "        \"title\": \"Wake up to WonderWidgets!\", \n" + "        \"type\": \"all\"\n" + "      }, \n"
                    + "      {\n" + "        \"items\": [\n" + "          \"Why <em>WonderWidgets</em> are great\", \n"
                    + "          \"Who <em>buys</em> WonderWidgets\"\n" + "        ], \n" + "        \"title\": \"Overview\", \n"
                    + "        \"type\": \"all\"\n" + "      }\n" + "    ], \n" + "    \"title\": \"Sample Slide Show\"\n"
                    + "  }\n" + "}\n";
            assertEquals(expected, body);
        } else if ("xml".equals(type)) {
            final String body = resp.getString("body");
            final String expected = "<?xml version='1.0' encoding='us-ascii'?>\n" + "\n" + "<!--  A SAMPLE set of slides  -->\n"
                    + "\n" + "<slideshow \n" + "    title=\"Sample Slide Show\"\n" + "    date=\"Date of publication\"\n"
                    + "    author=\"Yours Truly\"\n" + "    >\n" + "\n" + "    <!-- TITLE SLIDE -->\n"
                    + "    <slide type=\"all\">\n" + "      <title>Wake up to WonderWidgets!</title>\n" + "    </slide>\n" + "\n"
                    + "    <!-- OVERVIEW -->\n" + "    <slide type=\"all\">\n" + "        <title>Overview</title>\n"
                    + "        <item>Why <em>WonderWidgets</em> are great</item>\n" + "        <item/>\n"
                    + "        <item>Who <em>buys</em> WonderWidgets</item>\n" + "    </slide>\n" + "\n" + "</slideshow>";
            assertEquals(expected, body);
        } else if ("html".equals(type)) {
            final String body = resp.getString("body");
            final String expected = "<!DOCTYPE html>\n" + "<html>\n" + "  <head>\n" + "  </head>\n" + "  <body>\n"
                    + "      <h1>Herman Melville - Moby-Dick</h1>\n" + "\n" + "      <div>\n" + "        <p>\n"
                    + "          Availing himself of the mild, summer-cool weather that now reigned in these latitudes, and in preparation for the peculiarly active pursuits shortly to be anticipated, Perth, the begrimed, blistered old blacksmith, had not removed his portable forge to the hold again, after concluding his contributory work for Ahab's leg, but still retained it on deck, fast lashed to ringbolts by the foremast; being now almost incessantly invoked by the headsmen, and harpooneers, and bowsmen to do some little job for them; altering, or repairing, or new shaping their various weapons and boat furniture. Often he would be surrounded by an eager circle, all waiting to be served; holding boat-spades, pike-heads, harpoons, and lances, and jealously watching his every sooty movement, as he toiled. Nevertheless, this old man's was a patient hammer wielded by a patient arm. No murmur, no impatience, no petulance did come from him. Silent, slow, and solemn; bowing over still further his chronically broken back, he toiled away, as if toil were life itself, and the heavy beating of his hammer the heavy beating of his heart. And so it was.—Most miserable! A peculiar walk in this old man, a certain slight but painful appearing yawing in his gait, had at an early period of the voyage excited the curiosity of the mariners. And to the importunity of their persisted questionings he had finally given in; and so it came to pass that every one now knew the shameful story of his wretched fate. Belated, and not innocently, one bitter winter's midnight, on the road running between two country towns, the blacksmith half-stupidly felt the deadly numbness stealing over him, and sought refuge in a leaning, dilapidated barn. The issue was, the loss of the extremities of both feet. Out of this revelation, part by part, at last came out the four acts of the gladness, and the one long, and as yet uncatastrophied fifth act of the grief of his life's drama. He was an old man, who, at the age of nearly sixty, had postponedly encountered that thing in sorrow's technicals called ruin. He had been an artisan of famed excellence, and with plenty to do; owned a house and garden; embraced a youthful, daughter-like, loving wife, and three blithe, ruddy children; every Sunday went to a cheerful-looking church, planted in a grove. But one night, under cover of darkness, and further concealed in a most cunning disguisement, a desperate burglar slid into his happy home, and robbed them all of everything. And darker yet to tell, the blacksmith himself did ignorantly conduct this burglar into his family's heart. It was the Bottle Conjuror! Upon the opening of that fatal cork, forth flew the fiend, and shrivelled up his home. Now, for prudent, most wise, and economic reasons, the blacksmith's shop was in the basement of his dwelling, but with a separate entrance to it; so that always had the young and loving healthy wife listened with no unhappy nervousness, but with vigorous pleasure, to the stout ringing of her young-armed old husband's hammer; whose reverberations, muffled by passing through the floors and walls, came up to her, not unsweetly, in her nursery; and so, to stout Labor's iron lullaby, the blacksmith's infants were rocked to slumber. Oh, woe on woe! Oh, Death, why canst thou not sometimes be timely? Hadst thou taken this old blacksmith to thyself ere his full ruin came upon him, then had the young widow had a delicious grief, and her orphans a truly venerable, legendary sire to dream of in their after years; and all of them a care-killing competency.\n"
                    + "        </p>\n" + "      </div>\n" + "  </body>\n" + "</html>";
            assertEquals(expected, body);
        }
    }

    @Test
    void testBodyFormData() {
        config.getDataset().setHasBody(true);

        RequestBody body = new RequestBody();
        body.setType(RequestBody.Type.FORM_DATA);
        body.setParams(Arrays.asList(new Param("form_data_1", "<000 001"), new Param("form_data_2", "<000 002")));
        config.getDataset().setBody(body);
        config.getDataset().setMethodType(HttpMethod.POST);
        config.getDataset().setResource("post");
        config.getDataset().setFormat(Format.JSON);

        final Iterator<Record> respIt = recordBuilderService.buildFixedRecord(service.execute(config), config);

        final Record resp = respIt.next();
        assertFalse(respIt.hasNext());

        assertEquals("<000 001", resp.getRecord("body").getRecord("form").getString("form_data_1"));
        assertEquals("<000 002", resp.getRecord("body").getRecord("form").getString("form_data_2"));
    }

    @Test
    void testBodyXwwwformURLEncoded() {
        config.getDataset().setHasBody(true);

        RequestBody body = new RequestBody();
        body.setType(RequestBody.Type.X_WWW_FORM_URLENCODED);
        body.setParams(Arrays.asList(new Param("form_data_1", "<000 001"), new Param("form_data_2", "<000 002")));
        config.getDataset().setBody(body);
        config.getDataset().setMethodType(HttpMethod.POST);
        config.getDataset().setResource("post");
        config.getDataset().setFormat(Format.JSON);

        final Iterator<Record> respIt = recordBuilderService.buildFixedRecord(service.execute(config), config);

        final Record resp = respIt.next();
        assertFalse(respIt.hasNext());

        assertEquals("<000 001", resp.getRecord("body").getRecord("form").getString("form_data_1"));
        assertEquals("<000 002", resp.getRecord("body").getRecord("form").getString("form_data_2"));
    }

}
