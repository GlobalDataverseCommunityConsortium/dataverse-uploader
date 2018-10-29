/** *****************************************************************************
 * Copyright 2014, 2016 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************** */
package org.sead.uploader.clowder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.swing.border.EmptyBorder;

import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.cookie.Cookie;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.sead.uploader.util.FileResource;
import org.sead.uploader.util.PublishedFolderProxyResource;
import org.sead.uploader.util.PublishedResource;
import org.sead.uploader.util.Resource;
import org.sead.uploader.util.ResourceFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

/**
 *
 */
public class SEADTester {

    public static final String FRBR_EO = "http://purl.org/vocab/frbr/core#embodimentOf";
    private static final String DCTERMS_HAS_PART = "http://purl.org/dc/terms/hasPart";

    private static long max = 9223372036854775807l;
    private static boolean merge = false;
    private static boolean verify = false;
    private static boolean importRO = false;
    private static boolean sead2space = false;
    private static String sead2datasetId = null;

    private static long globalFileCount = 0l;
    private static long totalBytes = 0L;

    protected static boolean listonly = false;

    protected static Set<String> excluded = new HashSet<String>();
    ;

	private static String server = null;

    static PrintWriter pw = null;

    static HttpClientContext localContext = null;

    private static ResourceFactory rf = null;

    private static HashMap<String, String> roDataIdToNewId = new HashMap<String, String>();
    private static HashMap<String, String> roCollIdToNewId = new HashMap<String, String>();
    private static HashMap<String, String> roFolderProxy = new HashMap<String, String>();

    private static String CLOWDER_DEFAULT_VOCAB = "https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld";

    // Create a local instance of cookie store
    static CookieStore cookieStore = new BasicCookieStore();

    public static void main(String args[]) throws Exception {

        File outputFile = new File("SEADUploadLog_"
                + System.currentTimeMillis() + ".txt");
        try {
            pw = new PrintWriter(new FileWriter(outputFile));
        } catch (Exception e) {
            println(e.getMessage());
        }
        server = "http://localhost:9000";

        localContext = SEADAuthenticator.UPAuthenticate(server);
        // Create local HTTP context
        BasicClientCookie bc = new BasicClientCookie("id", "e601a866dd9faa8baa9a9aa40b770031e4303e9a6f3d10b54a59ba0a77d75815a4ce919ea99021fcb846f9bbfe106208d8d9b6468f8ec8b03753812ce4faea720b4e137b55665b10ac1bef1f8f12a0d63f2f828f3dc5130b6ff9042824c4c786902ec6a35e8deda741daf53abe4deaabf9587457143f69f466af09ebf3cf8208");
        bc.setDomain("localhost");
        cookieStore.addCookie(bc);
        localContext.setCookieStore(cookieStore);

        CloseableHttpClient httpclient = HttpClients
                .createDefault();

        postSingleMetadata(
                new JSONObject("{\"a\":\"rá\"}"),
                //new JSONObject("{\"a\":\"r\"}"),
                new JSONArray("[\"" + CLOWDER_DEFAULT_VOCAB + "\",{\"a\":\"http://purl.org/dc/terms/audience\"}]"),
                new JSONObject("{\"c\":\"d\"}"), "http://localhost:9000/api/metadata.jsonld", httpclient);
        if (pw != null) {
            pw.flush();
            pw.close();
        }
    }

    private static void postSingleMetadata(JSONObject singleContent,
            JSONArray singleContext, JSONObject agent, String uri,
            CloseableHttpClient httpclient) throws IOException {
        HttpEntity resEntity = null;
        try {
            //singleContext.put(CLOWDER_DEFAULT_VOCAB);
            JSONObject meta = new JSONObject();
            meta.put("content", singleContent);
            meta.put("dataset_id", "583314110d15772a7e37cd90");
            meta.put("@context", singleContext);
            //meta.put("agent", agent);

            //StringEntity se2 = new StringEntity(meta.toString());
            StringEntity se2 = new StringEntity(
                    "{\"@context\":[\"https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld\",{\"Funding Institution\":\"http://sead-data.net/terms/FundingInstitution\"}],\"dataset_id\":\"583314110d15772a7e37cd90\",\"content\":{\"Funding Institution\":\"rá\"}}", "UTF-8");
            println(meta.toString(2));

            //se2.setContentEncoding("UTF-8");
            se2.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,
                    "application/json"));

            HttpHost proxy = new HttpHost("127.0.0.1", 8888, "http");

            RequestConfig config = RequestConfig.custom()
                    .setProxy(proxy)
                    .build();
            HttpPost metadataPost = new HttpPost(uri);

            metadataPost.setHeader("X-Requested-With", "XMLHttpRequest");
            metadataPost.setHeader("Origin", "http://localhost:9000");

            metadataPost.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36");
            metadataPost.setHeader("Referer", "http://localhost:9000/datasets/583314110d15772a7e37cd90");
            metadataPost.setHeader("DNT", "1");
            metadataPost.setHeader("Cache-Control", "no-cache");

            metadataPost.setConfig(config);

            metadataPost.setEntity(se2);

            CloseableHttpResponse mdResponse = httpclient.execute(metadataPost,
                    localContext);

            resEntity = mdResponse.getEntity();
            if (mdResponse.getStatusLine().getStatusCode() != 200) {
                println("Error response when processing key="
                        + singleContent.keys().next() + " : "
                        + mdResponse.getStatusLine().getReasonPhrase());
                println("Value: " + singleContent.get(singleContent.keys().next().toString()).toString());
                println("Details: " + EntityUtils.toString(resEntity));
                throw new IOException("Non 200 response");
            }
        } finally {
            EntityUtils.consumeQuietly(resEntity);
        }

    }

    public static void println(String s) {
        System.out.println(s);
        System.out.flush();
        if (pw != null) {
            pw.println(s);
            pw.flush();
        }
        return;
    }
}
