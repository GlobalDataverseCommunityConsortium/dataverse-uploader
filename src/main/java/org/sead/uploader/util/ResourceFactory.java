/** *****************************************************************************
 * Copyright 2016 University of Michigan
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
package org.sead.uploader.util;

import com.apicatalog.jsonld.JsonLd;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.sead.uploader.clowder.SEADUploader;

import javax.json.Json;
import javax.json.JsonStructure;

import com.apicatalog.jsonld.api.JsonLdError;
import com.apicatalog.jsonld.document.JsonDocument;
import java.io.StringReader;
import java.math.BigDecimal;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.stream.Collectors;
import javax.json.JsonArray;
import javax.json.JsonObject;

public class ResourceFactory {

    public static final HashMap<String, String> grayConversions = new HashMap<String, String>() {
        {

            put("Creation Date",
                    "http://sead-data.net/terms/originalcreationdate");
            put("Uploaded By", "http://purl-data.net/terms/originalsubmitter");
            put("Date", "http://sead-data.net/terms/originalcreationdate");
            put("Instance Of", "http://sead-data.net/terms/originaldiskpath");
            put("External Identifier",
                    "http://sead-data.net/terms/publishedidoforiginal");
        }
    };

    public static final HashMap<String, String> graySwaps = new HashMap<String, String>() {
        {

            put("Creation Date", "Original Creation Date");
            put("Uploaded By", "Originally Uploaded By");
            put("Date", "Original Creation Date");
            put("Instance Of", "Original Disk Path");
            put("External Identifier", "Persistent Identifier of Original");
        }
    };

    JsonObject baseContext = Json.createObjectBuilder()
            .add("describes", "http://www.openarchives.org/ore/terms/describes")
            .add("aggregates", "http://www.openarchives.org/ore/terms/aggregates")
            .add("Has Part", "http://schema.org/hasPart")
            .add("Version","http://schema.org/version")
            .add("Title","http://purl.org/dc/terms/title")
            .add("Label","http://schema.org/name")
            .add("Mimetype","http://schema.org/fileFormat")
            .add("similarTo","http://schema.org/sameAs")
            .add("Size","https://dataverse.org/schema/core#filesize")
            .build();
    
    JSONObject orgJsonbaseContext = new JSONObject()
            .put("describes", "http://www.openarchives.org/ore/terms/describes")
            .put("aggregates", "http://www.openarchives.org/ore/terms/aggregates")
            .put("Has Part", "http://schema.org/hasPart")
            .put("Version","http://schema.org/version")
            .put("Title","http://purl.org/dc/terms/title")
            .put("Label","http://schema.org/name")
            .put("Mimetype","http://schema.org/fileFormat")
            .put("similarTo","http://schema.org/sameAs")
            .put("Size","https://dataverse.org/schema/core#filesize");
            
    JSONObject oremap;
    ArrayList<String> index;
    JSONArray aggregates;
    String rootPath;

    private CloseableHttpClient client;

    public ResourceFactory() {
        
    }
    
    public ResourceFactory(URL oremapURL) {
        String mapString = "";
        client = HttpClients.custom().build();
        try {
            if (oremapURL.getProtocol().equals("file")) {
                mapString = new BufferedReader(
                        new InputStreamReader(oremapURL.openStream(), UTF_8))
                        .lines()
                        .collect(Collectors.joining("\n"));
            } else {
                

                HttpEntity he = getURI(oremapURL.toURI());
                mapString = EntityUtils.toString(he, "UTF-8");
            }
        } catch (URISyntaxException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        createOreMapFromString(mapString);
    }
    
    void createOreMapFromString(String mapString) {
        JsonStructure struct;
        struct = Json.createReader(new StringReader(mapString)).readObject();
        JsonDocument doc = JsonDocument.of(struct);
        JsonArray array = null;
        try {
            array = JsonLd.expand(doc).get();
            JsonStructure context;
            mapString = JsonUtils.stringify(JsonLd.compact(JsonDocument.of(array), JsonDocument.of(baseContext)).get());
        } catch (JsonLdError e) {
            System.out.println(e.getMessage());
        }
 
System.out.println(mapString);

        oremap = new JSONObject(mapString);
        // private ArrayList<String> indexResources(String aggId, JSONArray
        // aggregates) {
        JSONObject aggregation = oremap.getJSONObject("describes");
        String aggId = aggregation.getString("@id");
        if(aggregation.has("Version")) {
        aggId = (aggId + "v" + aggregation.getString("Version")).replace(":?.?", "-");
        }
        
        rootPath = "/" + aggId + "/data/" + aggregation.getString("Title");
        aggregates = aggregation.getJSONArray("aggregates");
        ArrayList<String> l = new ArrayList<String>(aggregates.length() + 1);
        l.add(aggId);
        for (int i = 0; i < aggregates.length(); i++) {
            l.add(aggregates.getJSONObject(i).getString("@id"));
        }
        index = l;
        PublishedResource.setResourceFactory(this);

    }

    HttpEntity getURI(URI uri) {
        int tries = 0;
        while (tries < 5) {
            try {
                HttpGet getResource = new HttpGet(uri);
                getResource.setHeader("Content-type", "application/json;charset=utf-8");

                CloseableHttpResponse response;
                response = client.execute(getResource);
                if (response.getStatusLine().getStatusCode() == 200) {
                    return response.getEntity();
                }
                tries++;

            } catch (ClientProtocolException e) {
                tries += 5;
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // Retry if this is a potentially temporary error such
                // as a timeout
                tries++;
                /*				log.warn("Attempt# " + tries + " : Unable to retrieve file: "
						+ uri, e);
				if (tries == 5) {
					log.error("Final attempt failed for " + uri);
				}
                 */
                e.printStackTrace();
            }
        }
        return null;

    }

    public Resource getPublishedResource(String id, String parentPath) {
        int i = index.indexOf(id);
        PublishedResource p = new PublishedResource(
                aggregates.getJSONObject(i - 1));
        String path = parentPath + "/" + p.getName();
        p.setAbsolutePath(rootPath + "/" + path);
        p.setPath(path);
        return p;
    }

    public PublishedResource getParentResource() {
        JSONObject agg = oremap.getJSONObject("describes");
        agg.remove("aggregates");
        PublishedResource p = new PublishedResource(agg);
        p.setAbsolutePath(rootPath);
        p.setPath(p.getName());
        return p;
    }

    static HashMap<String, String> grayContext = null;

    public String getURIForContextEntry(String key) {
        if (grayContext == null) {
            grayContext = new HashMap<String, String>();
            for (String oldKey : graySwaps.keySet()) {
                grayContext.put(graySwaps.get(oldKey),
                        grayConversions.get(oldKey));
            }
        }
        String uri = null;
        if (oreTerms.contains(key)) {
            uri = orePredBaseString + key;
        } else if ("Metadata on Original".equals(key)) {
            uri = "http://sead-data.net/terms/sourceMetadata";
        } else if ("Geolocation".equals(key)) {
            uri = "http://sead-data.net/terms/hasGeolocation";
        } else if ("Latitude".equals(key)) {
            uri = "http://www.w3.org/2003/01/geo/wgs84_pos#lat";
        } else if ("Longitude".equals(key)) {
            uri = "http://www.w3.org/2003/01/geo/wgs84_pos#long";
        } else if ("Upload Path".equals(key)) {
            uri = SEADUploader.FRBR_EO;
        } else if (grayContext.containsKey(key)) {
            uri = grayContext.get(key);
        } else {
            Object context = oremap.get("@context");
            uri = getURIForContextEntry(context, key);
        }
        return uri;
    }

    private String[] words = {"describes", "AggregatedResource",
        "Aggregation", "ResourceMap", "similarTo", "aggregates"};
    private HashSet<String> oreTerms = new HashSet<String>(Arrays.asList(words));

    private String orePredBaseString = "http://www.openarchives.org/ore/terms/";

    private String getURIForContextEntry(Object context, String key) {
        String uri = null;
        if (context instanceof JSONArray) {
            for (int i = 0; i < ((JSONArray) context).length(); i++) {
                uri = getURIForContextEntry(((JSONArray) context).get(i), key);
                if (uri != null) {
                    return uri;
                }
            }
        } else if (context instanceof JSONObject) {
            if (((JSONObject) context).has(key)) {

                // FixMe - support values that are objects with an @id entry...
                uri = ((JSONObject) context).getString(key);
            }
        }
        if (grayConversions.containsKey(key)) {
            uri = grayConversions.get(key);
        }
        return uri;
    }

}
