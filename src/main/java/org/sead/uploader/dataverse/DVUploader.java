/** *****************************************************************************
 * Copyright 2020 GDCC, 2018 Texas Digital Library, jim.myers@computer.org
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
package org.sead.uploader.dataverse;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonWriter;
import jakarta.json.JsonWriterFactory;
import jakarta.json.stream.JsonGenerator;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.DigestInputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipFile;


import javax.net.ssl.SSLContext;

import org.apache.commons.codec.binary.Hex;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.sead.uploader.AbstractUploader;
import static org.sead.uploader.AbstractUploader.println;
import org.sead.uploader.util.BagResourceFactory;
import org.sead.uploader.util.PublishedResource;
import org.sead.uploader.util.UploaderException;
import org.sead.uploader.util.Resource;

/**
 * The Dataverse Uploader supports the upload of files/directories from a local
 * disk.
 *
 */
public class DVUploader extends AbstractUploader {

    private static String apiKey = null;
    private static String datasetPID = null;
    private static String alias = null;
    private static boolean oldServer = false;
    private static int maxWaitTime = 60;
    private static boolean recurse = false;
    private static boolean directUpload = true;
    private static boolean trustCerts = false;
    private static boolean singleFile = false;

    private int timeout = 1200;
    private int httpConcurrency = 4;

    //private static long mpSizeLimit = 5 * 1024 * 1024;
    private RequestConfig config = RequestConfig.custom()
            .setConnectTimeout(timeout * 1000)
            .setConnectionRequestTimeout(timeout * 1000)
            .setSocketTimeout(timeout * 1000)
            .setCookieSpec(CookieSpecs.STANDARD)
            .setExpectContinueEnabled(true)
            .build();

    private static HttpClientContext localContext = HttpClientContext.create();

    private PoolingHttpClientConnectionManager cm = null;

    public static void main(String args[]) throws Exception {

        setUploader(new DVUploader());
        uploader.createLogFile("DVUploaderLog_");
        uploader.setSpaceType("Dataverse");
        println("\n----------------------------------------------------------------------------------\n");
        println("TTTTT  DDD   L    Texas");
        println("  T    D  D  L    Digital");
        println("  T    DDD   LLL  Library");
        println("");
        println("DVUploader - a command-line application to upload files to any Dataverse Dataset");
        println("Developed with support from TDL and GDCC for the Dataverse Community");
        println("See the wiki at https://github.com/GlobalDataverseCommunityConsortium/dataverse-uploader for information on using DVUploader.");
        
        println("\n----------------------------------------------------------------------------------\n");
        println(String.join(" ", args).replaceAll("key=[0-9a-fA-F\\-]+", "key=<apiKey>"));
        println("\n***Parsing arguments:***\n");
        uploader.parseArgs(args);
        if (server == null || datasetPID == null || apiKey == null || (requests.isEmpty() && !uploader.getImportRO())) {
            println("\n***Required arguments not found.***");
            usage();
        } else {
            println("\n***Starting to Process Upload Requests:***\n");
            uploader.processRequests();
        }
        println("\n***Execution Complete.***");
        println("\n***If any errors were shown, you may want to rerun DVUploader to assure that all files uploaded. For persistent issues, contact your Dataverse admin.***");
        println("\n***Execution Complete.***");
        
    }

    private static void usage() {
        println("\nUsage:");
        println("  java -jar DVUploader-1.2.0.jar -server=<serverURL> -key=<apikey> -did=<dataset DOI> <files or directories>");

        println("\n  where:");
        println("      <serverUrl> = the URL of the server to upload to, e.g. https://datverse.tdl.org");
        println("      <apiKey> = your personal apikey, created in the dataverse server at <serverUrl>");
        println("      <did> = the Dataset DOI you are uploading to, e.g. doi:10.5072/A1B2C3");
        println("      <files or directories> = a space separated list of files to upload or directory name(s) where the files to upload are");
        println("\n  Optional Arguments:");
        println("      -uploadviaserver      - Use Dataverse's indirect upload capability. This makes temporary copies of files on the Dataverse server, puts a greater workload on the server and is not suitable for large files, but it can be used when direct uploads are not enabled for a given dataset.");
        println("      -listonly          - Scan the Dataset and local files and list what would be uploaded (does not upload with this flag)");
        println("      -limit=<n>         - Specify a maximum number of files to upload per invocation.");
        println("      -verify            - Check both the file name and checksum in comparing with current Dataset entries.");
        println("      -skip=<n>          - a number of files to skip before starting processing (saves time when you know the first n files have been uploaded before)");
        println("      -recurse           - recurse into subdirectories");
        println("      -maxlockwait       - the maximum time to wait (in seconds) for a Dataset lock (i.e. while the last file is ingested) to expire (default 60 seconds)");
        println("      -trustall          - trust all server certificates (i.e. for use when testing with self-signed server certificates)");
        println("      -singlefile        - send each file to the server separately (only affects directupload/ when -uploadviaserver is not set)");
        println("      -bag=<URL>         - 'alpha' capbility to create a dataset from a Bag exported by Dataverse. <URL> is the location of the Bag to process.");
        println("      -createIn=<alias>  - required for Bag import: the alias of the Dataverse you want to create a dataset in");
        println("");
        println("See https://github.com/GlobalDataverseCommunityConsortium/dataverse-uploader/wiki/DVUploader,-a-Command-line-Bulk-Uploader-for-Dataverse for more usage information.");
        println("");

    }

    @Override
    public boolean parseCustomArg(String arg) {

        if (arg.startsWith("-key")) {
            apiKey = arg.substring(arg.indexOf(argSeparator) + 1);
            println("Using apiKey: " + apiKey);
            return true;
        } else if (arg.startsWith("-did")) {
            datasetPID = arg.substring(arg.indexOf(argSeparator) + 1);
            println("Adding content to: " + datasetPID);
            return true;
        } else if (arg.startsWith("-createIn")) {
            alias = arg.substring(arg.indexOf(argSeparator) + 1);
            println("Will create a Dataset in Dataverse: " + alias);
            return true;
        } else if (arg.equals("-recurse")) {
            recurse = true;
            println("Will recurse into subdirectories");
            return true;
        } else if (arg.equals("-uploadviaserver")) {
            directUpload = false;
            println("Will not use direct upload of files (not recommended for large files)");
            return true;
        } else if (arg.startsWith("-bag")) {
                importRO = true;
                try {
                    bagLocation = new URL(arg.substring(arg.indexOf(argSeparator) + 1));
                } catch (MalformedURLException e) {
                    println("Unable to interpret " + arg.substring(arg.indexOf(argSeparator) + 1) + " as a URL. Exiting.");
                    System.exit(0);
                }
                println("RO Mode: URL for RDA Bag is : " + bagLocation.toString());
        } else if (arg.equals("-trustall")) {
            trustCerts = true;
            println("Will trust all certificates");
            return true;
        } else if (arg.equals("-singlefile")) {
            singleFile = true;
            println("Will send files individually");
            return true;
        } else if (arg.startsWith("-maxlockwait")) {
            try {
                maxWaitTime = Integer.parseInt(arg.substring(arg.indexOf(argSeparator) + 1));
                println("Setting max wait time for locks to :" + maxWaitTime + " seconds");
            } catch (NumberFormatException nfe) {
                println("Unable to parse max wait time for locks, using default (60 seconds)");
            }
            return true;
        }
        return false;
    }

    private ZipFile zf = null;

    @Override
    public void importBag(URL bagLocation) throws URISyntaxException {
        rf = new BagResourceFactory(bagLocation);
        doImportRO();
    }
    
    @Override
    public HttpClientContext authenticate() {
        return new HttpClientContext();
    }

    public CloseableHttpClient getSharedHttpClient() {
        if (httpclient == null) {
            try {
                initHttpPool();
                httpclient = HttpClients.custom()
                        .setConnectionManager(cm)
                        .setDefaultRequestConfig(config)
                        .build();

            } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException ex) {
                Logger.getLogger(DVUploader.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return httpclient;
    }

    HashMap<String, JSONObject> existingItems = null;
    boolean datasetMDRetrieved = false;

    CloseableHttpClient httpclient = null;

    
    /**
     *
     * @param path - the current path to the item
     * @param item - the local item to find
     * @return
     */
    @Override
    public String itemExists(String path, Resource item) {
        String tagId = null;

        String relPath = path;
        if (importRO) {
            // remove the '/<ro_id>/data' prefix on imported paths to make
            // it match the file upload paths
            println("in" + path);
                            relPath= relPath.substring(relPath.indexOf("/data") + 5);
                            println("out:" + relPath);
        }

        String sourcepath = item.getName();

        // One-time: get metadata for dataset to see if it exists and what files it
        // contains
        if (!datasetMDRetrieved) {
            httpclient = getSharedHttpClient();

            try {
                // This api call will find the dataset and, if found, retrieve the list of files
                // in the current version (the only one we can add to)
                // http://$SERVER/api/datasets/$id/versions/$versionId/files?key=$apiKey

                String serviceUrl = server + "/api/datasets/:persistentId/versions/:latest/files?key=" + apiKey
                        + "&persistentId=" + datasetPID;
                HttpGet httpget = new HttpGet(serviceUrl);

                CloseableHttpResponse response = httpclient.execute(httpget, getLocalContext());
                JSONArray datafileList = null;
                try {
                    switch (response.getStatusLine().getStatusCode()) {
                        case 200:
                            HttpEntity resEntity = response.getEntity();
                            if (resEntity != null) {
                                String res = EntityUtils.toString(resEntity);
                                datafileList = (new JSONObject(res)).getJSONArray("data");
                                existingItems = new HashMap<>();
                            }
                            break;
                        case 404:
                            println("Dataset Not Found: " + datasetPID);
                            break;
                        default:
                            // Report unexpected errors and assume dataset doesn't exist
                            println("Error response when checking for existing item at " + sourcepath + " : "
                                    + response.getStatusLine().getReasonPhrase());
                            break;
                    }
                } finally {
                    response.close();
                }
                boolean convertedFiles = false;
                if (datafileList != null) {
                    for (int i = 0; i < datafileList.length(); i++) {
                        JSONObject entry = datafileList.getJSONObject(i);
                        JSONObject df = entry.getJSONObject("dataFile");
                        if (df.has("originalFileFormat")
                                && (!df.getString("contentType").equals(df.getString("originalFileFormat")))) {
                            println("The file named " + df.getString("filename")
                                    + " on the server was created by Dataverse's ingest process from an original uploaded file");
                            convertedFiles = true;
                        }
                        String filepath = df.getString("filename");
                        if(entry.has("directoryLabel")) {
                            filepath=entry.get("directoryLabel") + "/" + filepath;
                        }
                        println("Recording: " + filepath);
                        existingItems.put(filepath, df.getJSONObject("checksum"));
                    }
                    if (convertedFiles) {
                        println("*****   DVUploader cannot detect attempts to re-upload files to Dataverse when Dataverse has created a derived file during ingest such as those listed above.");
                        println("*****   You may see upload errors for any file where ingest would re-create one of these files.");
                    }

                }

            } catch (IOException e) {
                println("Error processing check on " + sourcepath + " : " + e.getMessage());
            } finally {
                datasetMDRetrieved = true;
            }

        }

        if (relPath.equals("/")) {
            // existingItems will exist if we found the dataset
            if (existingItems != null) {
                if (item.isDirectory()) {
                    // Looking for the dataset itself
                    tagId = datasetPID;
                } else {
                    // A single file on the command line
                    if (existingItems.containsKey(sourcepath)) {
                        JSONObject checksum = existingItems.get(sourcepath);
                        tagId = checksum.getString("type") + ":" + checksum.getString("value");
                    }

                }
            }
        } else {
            // Looking for an item in the local directory structure
            if (item.isDirectory()) {
                // Directories aren't yet represented in Dataverse
                return null;
            } else {
                // A file within the local directory
                if ((existingItems != null) && existingItems.containsKey(sourcepath)) {
                    JSONObject checksum = existingItems.get(sourcepath);
                    tagId = checksum.getString("type") + ":" + checksum.getString("value");
                }
            }
        }
        if (verify && (tagId != null) && (!item.isDirectory())) {
            tagId = verifyDataByHash(tagId, path, item);
        }
        return (tagId);
    }

    static HashMap<String, String> hashIssues = new HashMap<String, String>();

    @Override
    protected String verifyDataByHash(String tagId, String path, Resource item) {
        JSONObject checksum = existingItems.get(item.getName());
        if (!checksum.getString("value").equals(item.getHash(checksum.getString("type")))) {
            hashIssues.put(path + item.getName(), "!!!: A different version of this item exists with ID: " + tagId);
            return null;
        } // else it matches!
        return tagId;
    }

    
    
   
    
//    call Create dataset, 
//    add semantic metadata
//    add file metadata when/after creating file
      
    
    @Override
    protected String preprocessCollection(Resource dir, String path, String parentId, String collectionId) throws UploaderException {
        // DV - create the dataset or add metadata
    println("Preproc: " + dir.getName());    
        if (importRO) {
            if (!listonly) {
                if (collectionId == null) {
                    if (parentId == null) {
                        collectionId = createDataset(dir, path);
                        datasetPID = collectionId;
                    } else {
                        //    ToDo - folder - folders don't exist - add a md file if there is folder md
                    }
                } else {
                    // We already have the dataset uploaded so record it's id
                    if (parentId == null) {
                        addDatasetMetadata(dir);
                    }
                }
            }
            if (datasetPID != null) {
                println("Dataset ID: " + datasetPID);
            }
        }
        if (!path.equals("/" + dir.getName().trim()) && !recurse) {
            throw new UploaderException("              DVUploader is not configured to recurse into sub-directories.");
        }
        return collectionId;
    }
    
    private String createDataset(Resource dir, String path) {
        println("In Create");
        httpclient = getSharedHttpClient();
        try {
            String urlString = server + "/api/dataverses/" + alias + "/datasets/:startmigration";
            urlString = urlString + "?key=" + apiKey;
            println("Calling " + urlString);
            HttpPost httppost = new HttpPost(urlString);
            StringEntity se = null;
            if(dir instanceof PublishedResource) {
                println("Sending: " + ((PublishedResource)dir).getMetadata().toString(2));
              se = new StringEntity(((PublishedResource)dir).getMetadata().toString(2), "utf-8");
            } else {
                //ToDo - support creating a dataset from a plain directory
            }
            httppost.setEntity(se);
            httppost.addHeader("Content-Type","application/json-ld");

            CloseableHttpResponse response = httpclient.execute(httppost, getLocalContext());
            try {
                if (response.getStatusLine().getStatusCode() == 201) {
                    HttpEntity resEntity = response.getEntity();
                    if (resEntity != null) {
                        String res = EntityUtils.toString(resEntity);
                        println("Create response: " + res);
                        datasetPID = Json.createReader(new StringReader(res)).readObject().getJsonObject("data").getString("persistentId");
                        datasetMDRetrieved = false;
                        return datasetPID;
                    }
                } else {
                    println("Oops: " + response.getStatusLine().getReasonPhrase());
                    println("Unable to continue processing");
                    System.exit(1);
                }
            } finally {
                EntityUtils.consumeQuietly(response.getEntity());
            }
        } catch (IOException e) {
            println(e.getMessage());
        }
        
        return null;
        
    }
    
    @Override
    public void addDatasetMetadata(String newSubject, String type, JSONObject relationships) {
         // TBD
        // println("DVUploader does not add relationships separately.");
    }

    @Override
    protected void postProcessChildren(Resource dir) {
        if (!singleFile && directUpload) {
            //Have to register all the files in this dir with Dataverse
            int retries = 5;
            //In case of prior 504 (or other) errors, make sure the dataset is OK before adding files
            int total = 0;
            // For new servers, wait up to maxWaitTime for a dataset lock to expire.
            try {
                while (isLocked() && (total < maxWaitTime)) {
                    TimeUnit.SECONDS.sleep(1);
                    total = total + 1;
                }
            } catch(InterruptedException ie) {
                println("Error waiting for Dataverse dataset lock - skipping: " + dir.getAbsolutePath());
                retries=0;
            }
                            
            String urlString = server + "/api/datasets/:persistentId/addFiles";
            urlString = urlString + "?persistentId=" + datasetPID + "&key=" + apiKey;
            
            while (retries > 0) {
                HttpPost httppost = new HttpPost(urlString);
                JSONArray jsonData = new JSONArray();
                // ContentBody bin = file.getContentBody();
                MultipartEntityBuilder meb = MultipartEntityBuilder.create();
                for (Resource file : dir.listResources()) {
                    if (!file.isDirectory()) {
                        //println("Adding " + file.getName() + " to list: " + file.getMetadata().toString(2));

                        jsonData.put(file.getMetadata());
                    }
                }
                meb.addTextBody("jsonData", jsonData.toString());

                HttpEntity reqEntity = meb.build();
                httppost.setEntity(reqEntity);
                try {
                    CloseableHttpResponse postResponse = httpclient.execute(httppost, getLocalContext());

                    int postStatus = postResponse.getStatusLine().getStatusCode();
                    String postRes = null;
                    HttpEntity postEntity = postResponse.getEntity();
                    if (postEntity != null) {
                        postRes = EntityUtils.toString(postEntity);
                        //println("Raw response: " +postRes);
                    }

                    if (postStatus == 200) {
                        JSONArray files = (new JSONObject(postRes)).getJSONObject("data")
                                .getJSONArray("Files");
                        JSONArray errArray = new JSONArray();
                        List<String> errIds = new ArrayList<String>();
                        for (int i = 0; i < files.length(); i++) {
                            JSONObject fileResult = files.getJSONObject(i);
                            if (fileResult.has("error Code: ")) {
                                errArray.put(fileResult);
                                errIds.add(fileResult.getString("storageIdentifier"));
                            }
                        }
                        println((jsonData.length() - errIds.size()) + " files successfully added from this folder");
                        if (!errIds.isEmpty()) {
                            println(errIds.size() + " files were not added. Please alert your Dataverse administrator:");
                            for (Resource file : dir.listResources()) {
                                if (!file.isDirectory()) {
                                    String id = file.getMetadata().getString("storageIdentifier");
                                    int i = errIds.indexOf(id);
                                    if (i != -1) {
                                        String msg = errArray.getJSONObject(i).getString("message");
                                        println("File: " + file.getName() + " failed with error: " + msg);
                                    }
                                }
                            }
                        }
                        retries = 0;
                        total = 0;
                        // For new servers, wait up to maxWaitTime for a dataset lock to expire.
                        while (isLocked() && (total < maxWaitTime)) {
                            TimeUnit.SECONDS.sleep(1);
                            total = total + 1;
                        }
                    } else if (postStatus == 400 && oldServer) {
                        // If the call to the lock API fails in isLocked(), oldServer will be set to
                        // true and
                        // all we can do for a lock is to keep retrying.
                        // Unfortunately, the error messages are configurable, so there's no guaranteed
                        // way to detect
                        // locks versus other conditions (e.g. file exists), so we can test for unique
                        // words in the default messages
                        if ((postRes != null) && postRes.contains("lock")) {
                            retries--;
                        } else {
                            println("Error response when processing files in " + dir.getAbsolutePath() + " : "
                                    + postResponse.getStatusLine().getReasonPhrase());
                            // A real error: e.g. This file already exists in the dataset.
                            if (postRes != null) {
                                println(postRes);
                            }
                            // Skip
                            retries = 0;
                        }
                    } else {
                        // An error and unlikely that we can recover, so report and move on.
                        println("Error response when processing files in " + dir.getAbsolutePath() + " : "
                                + postResponse.getStatusLine().getReasonPhrase());
                        if (postRes != null) {
                            println(postRes);
                        }
                        retries = 0;
                    }
                } catch (InterruptedException e) {
                    println("Error waiting for Dataverse dataset lock after: " + dir.getAbsolutePath());
                    retries = 0;
                } catch (IOException ex) {
                    retries = retries--;
                    println("Error registering files with Dataverse for : " + dir.getAbsolutePath() + " : " + ex.getMessage());
                }
            }
        }
    }

    @Override
    protected void postProcessCollection() {
        //importRO is the only time we are using the semantic / migrate API and have to call after uploading files (to trigger dataset release)
        if (importRO) {
            httpclient = getSharedHttpClient();
            // Now post data
            String urlString = server + "/api/datasets/:persistentId/actions/:releasemigrated";
            urlString = urlString + "?persistentId=" + datasetPID + "&key=" + apiKey;
            HttpPost httppost = new HttpPost(urlString);
            httppost.setHeader("Content-type", "application/json-ld");
            PublishedResource ds = rf.getParentResource();

            StringEntity body;
            try {
                JsonObject md = Json.createObjectBuilder().add("http://schema.org/datePublished", ds.getMetadata().getString("http://schema.org/datePublished")).build();
                
                			StringWriter sw = new StringWriter();
			Map<String, Object> properties = new HashMap<>(1);
			properties.put(JsonGenerator.PRETTY_PRINTING, true);
			JsonWriterFactory writerFactory = Json.createWriterFactory(properties);
			JsonWriter jsonWriter = writerFactory.createWriter(sw);
			jsonWriter.write(md);
			jsonWriter.close();
			String mdString = sw.toString();
                        
                body = new StringEntity(mdString, "utf-8");
println(mdString);
                httppost.setEntity(body);

                CloseableHttpResponse response = httpclient.execute(httppost, getLocalContext());

                int status = response.getStatusLine().getStatusCode();
                String res = null;
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    res = EntityUtils.toString(resEntity);
                }
                println("Status: " + status);
                if (status != 200) {
                    println("Error response trying to release migrated file " + ds.getIdentifier() + " : "
                            + response.getStatusLine().getReasonPhrase());
                    if(res!=null) {
                    println(res);
                    }
                }
            } catch (UnsupportedEncodingException ex) {
                //Not expected
                println("Unsupported encoding: " + ex.getMessage());
            } catch (IOException ex) {
                println("Error trying to release migrated file " + ds.getIdentifier() + " : "
                        + ex.getMessage());
            }

        }
    }

    @Override
    protected String postProcessChild(Resource dir, String path, String parentId, String collectionId) {
        // TBD
        // println("DVUploader does not need to post-process newly created items");
        return null;
    }

    @Override
    protected void postProcessDatafile(String newUri, String existingUri, String collectionId, Resource file,
            Resource dir) throws ClientProtocolException, IOException {
        // TBD
        // println("DVUploader does not need to post-process data files");

    }

    @Override
    protected HttpClientContext reauthenticate(long startTime) {
        // TBD
        // println("DVUploader does not need to reauthenticate");
        return getLocalContext();
    }

    @Override
    protected String uploadDatafile(Resource file, String path) {
        httpclient = getSharedHttpClient();
        String dataId = null;
        int retries = 5;
        if (directUpload) {
            try {
//@Deprecated -used in v4.20                    dataId = directFileUpload(file, path, retries);
                dataId = multipartDirectFileUpload(file, path, retries);
            } catch (IOException e) {
                println("Error processing request for storage id" + file.getAbsolutePath() + " : " + e.getMessage());
                retries = 0;
            }

        } else {
            while (retries > 0) {

                try {
                    // Now post data
                    String urlString = server + "/api/datasets/:persistentId/add";
                    urlString = urlString + "?persistentId=" + datasetPID + "&key=" + apiKey;
                    HttpPost httppost = new HttpPost(urlString);

                    ContentBody bin = file.getContentBody();

                    MultipartEntityBuilder meb = MultipartEntityBuilder.create();
                    meb.addPart("file", bin);
                    if (recurse) {
                        // Dataverse takes paths without an initial / and ending without a /
                        // with the path not including the file name
                        String parentPath = "";
                        if(!importRO) {
                            parentPath= path.substring(1, path.lastIndexOf("/"));
                        } else {
                            println(path);
                            parentPath= path.substring(path.indexOf("/data/") + 6);
                            println(parentPath);
                            parentPath= parentPath.substring(parentPath.indexOf("/"));
                            println(parentPath);
                            int index = parentPath.lastIndexOf("/");
                            if(index>=0) {
                            parentPath = parentPath.substring(0, index);
                            }
                            println("result:" + parentPath);
                    }

                        if (!parentPath.isEmpty()) {
                            println("pp" + parentPath);
                            meb.addTextBody("jsonData", "{\"directoryLabel\":\"" + parentPath + "\"}");
                        }
                    }

                    HttpEntity reqEntity = meb.build();
                    httppost.setEntity(reqEntity);

                    CloseableHttpResponse response = httpclient.execute(httppost, getLocalContext());
                    try {
                        int status = response.getStatusLine().getStatusCode();
                        String res = null;
                        HttpEntity resEntity = response.getEntity();
                        if (resEntity != null) {
                            res = EntityUtils.toString(resEntity);
                        }
                        if (status == 200) {
                            JSONObject checksum = (new JSONObject(res)).getJSONObject("data").getJSONArray("files")
                                    .getJSONObject(0).getJSONObject("dataFile").getJSONObject("checksum");
                            dataId = checksum.getString("type") + ":" + checksum.getString("value");
                            retries = 0;
                            int total = 0;
                            // For new servers, wait up to maxWaitTime for a dataset lock to expire.
                            while (isLocked() && (total < maxWaitTime)) {
                                TimeUnit.SECONDS.sleep(1);
                                total = total + 1;
                            }
                        } else if (status == 400 && oldServer) {
                            // If the call to the lock API fails in isLocked(), oldServer will be set to
                            // true and
                            // all we can do for a lock is to keep retrying.
                            // Unfortunately, the error messages are configurable, so there's no guaranteed
                            // way to detect
                            // locks versus other conditions (e.g. file exists), so we can test for unique
                            // words in the default messages
                            if ((res != null) && res.contains("lock")) {
                                retries--;
                            } else {
                                println("Error response when processing " + file.getAbsolutePath() + " : "
                                        + response.getStatusLine().getReasonPhrase());
                                // A real error: e.g. This file already exists in the dataset.
                                if (res != null) {
                                    println(res);
                                }
                                // Skip
                                retries = 0;
                            }
                        } else {
                            // An error and unlikely that we can recover, so report and move on.
                            println("Error response when processing " + file.getAbsolutePath() + " : "
                                    + response.getStatusLine().getReasonPhrase());
                            if (res != null) {
                                println(res);
                            }
                            retries = 0;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        retries = 0;
                    } finally {
                        EntityUtils.consumeQuietly(response.getEntity());
                    }

                } catch (IOException e) {
                    println("Error processing " + file.getAbsolutePath() + " : " + e.getMessage());
                    retries = retries - 1;
                }
            }
        }
        return dataId;

    }

    private boolean isLocked() {
        httpclient = getSharedHttpClient();
        try {
            String urlString = server + "/api/datasets/:persistentId/locks";
            urlString = urlString + "?persistentId=" + datasetPID + "&key=" + apiKey;
            HttpGet httpget = new HttpGet(urlString);

            CloseableHttpResponse response = httpclient.execute(httpget, getLocalContext());
            try {
                if (response.getStatusLine().getStatusCode() == 200) {
                    HttpEntity resEntity = response.getEntity();
                    if (resEntity != null) {
                        String res = EntityUtils.toString(resEntity);
                        boolean locked = (new JSONObject(res)).getJSONArray("data").length() > 0;
                        if (locked) {
                            println("Dataset locked - waiting...");
                        }
                        return locked;
                    }
                } else {
                    oldServer = true;
                    TimeUnit.SECONDS.sleep(1);
                    return false;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                EntityUtils.consumeQuietly(response.getEntity());
            }
        } catch (IOException e) {
            println(e.getMessage());
        }
        return false;
    }

    @Deprecated
    private String directFileUpload(Resource file, String path, int retries) throws IOException {
        String dataId = null;
        while (retries > 0) {
            // Now post data
            String urlString = server + "/api/datasets/:persistentId/uploadsid";
            urlString = urlString + "?persistentId=doi:" + datasetPID.substring(4) + "&key=" + apiKey;
            HttpGet httpget = new HttpGet(urlString);
            CloseableHttpResponse response = httpclient.execute(httpget, getLocalContext());
            try {
                int status = response.getStatusLine().getStatusCode();
                String uploadUrl = null;
                String jsonResponse = null;
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    jsonResponse = EntityUtils.toString(resEntity);
                }
                if (status == 200) {
                    JSONObject data = (new JSONObject(jsonResponse)).getJSONObject("data");
                    uploadUrl = data.getString("url");
                    String storageIdentifier = data.getString("storageIdentifier");

                    HttpPut httpput = new HttpPut(uploadUrl);

                    httpput.addHeader("x-amz-tagging", "dv-state=temp");
                    MessageDigest messageDigest = MessageDigest.getInstance("MD5");

                    try (InputStream inStream = file.getInputStream(); DigestInputStream digestInputStream = new DigestInputStream(inStream, messageDigest)) {
                        // This is hte new form for requests - keeping the example but won't update until we can change all
                        //HttpUriRequest httpput = RequestBuilder.put()
                        //    .setUri(uploadUrl)
                        //    .setHeader("x-amz-tagging", "dv-state=temp")
                        //    .setEntity(new InputStreamEntity(digestInputStream, file.length()))
                        //    .build();
                        httpput.setEntity(new InputStreamEntity(digestInputStream, file.length()));
                        CloseableHttpResponse putResponse = httpclient.execute(httpput);
                        try {
                            int putStatus = putResponse.getStatusLine().getStatusCode();
                            String putRes = null;
                            HttpEntity putEntity = putResponse.getEntity();
                            if (putEntity != null) {
                                putRes = EntityUtils.toString(putEntity);
                            }
                            if (putStatus == 200) {
                                String localchecksum = Hex.encodeHexString(digestInputStream.getMessageDigest().digest());
                                dataId = registerFileWithDataverse(file, path, storageIdentifier, localchecksum, retries);
                                retries = 0;
                            }
                        } catch (IOException e) {
                            e.printStackTrace(System.out);
                            println("Error processing POST to Dataverse" + file.getAbsolutePath() + " : " + e.getMessage());
                            retries = 0;
                        }
                    }
                } else {
                    if (status >= 500) {
                        retries=0;
                    }
                }

            } catch (IOException e) {
                e.printStackTrace(System.out);
                println("Error processing file upload " + file.getAbsolutePath() + " : " + e.getMessage());
                retries = 0;
            } catch (NoSuchAlgorithmException e1) {
                println("Checksum Algoritm not found: " + e1.getLocalizedMessage());
            }
        }
        return dataId;
    }

    private String multipartDirectFileUpload(Resource file, String path, int retries) throws IOException {
        String dataId = null;
        
        while (retries > 0) {        
        // Start multipart upload with a call to Dataverse. It will make a call to S3 to start the multipart upload and will return a set of presigned Urls for us to upload the parts
        String urlString = server + "/api/datasets/:persistentId/uploadurls";
        urlString = urlString + "?persistentId=doi:" + datasetPID.substring(4) + "&key=" + apiKey + "&size=" + file.length();
        HttpGet httpget = new HttpGet(urlString);
        CloseableHttpResponse response = httpclient.execute(httpget, getLocalContext());
            try {
                int status = response.getStatusLine().getStatusCode();

                String jsonResponse = null;
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    jsonResponse = EntityUtils.toString(resEntity);
                }
                if (status == 200) {
                    //println(jsonResponse);
                    JSONObject uploadResponse = (new JSONObject(jsonResponse)).getJSONObject("data");
                    //Along with the parts, which should be listed numerically, we get convenience URLs to call on Dataverse to abort or complete the multipart upload
                    //backwards compat in testing
                    long maxPartSize = 5 * 1024 * 1024l;
                    if (uploadResponse.has("partSize")) {
                        maxPartSize = uploadResponse.getLong("partSize");
                    }
                    if (uploadResponse.has("url")) {
                        String storageIdentifier = uploadResponse.getString("storageIdentifier");
                        String uploadUrl = uploadResponse.getString("url");
                        /*The .replace("%3B",";") in the next line is a work-around for Dell's Isolon storage S3 implementation which can't handle the endcoded char
                              in the X-Amz-SignedHeaders param. Currently this is the only place the encoded ; is present and we've confirmed that unencoding also works with Amazon's S3 stores.
                              If there are ever issues, we could create a -dell flag, only apply this to the specific param involved, etc. to drop this work-around when not needed.
                            */
                        HttpPut httpput = new HttpPut(uploadUrl.replace("%3B",";"));

                        httpput.addHeader("x-amz-tagging", "dv-state=temp");
                        try {
                            MessageDigest messageDigest = MessageDigest.getInstance("MD5");

                            try (InputStream inStream = file.getInputStream(); DigestInputStream digestInputStream = new DigestInputStream(inStream, messageDigest)) {
                                // This is hte new form for requests - keeping the example but won't update until we can change all
                                //HttpUriRequest httpput = RequestBuilder.put()
                                //    .setUri(uploadUrl)
                                //    .setHeader("x-amz-tagging", "dv-state=temp")
                                //    .setEntity(new InputStreamEntity(digestInputStream, file.length()))
                                //    .build();
                                httpput.setEntity(new InputStreamEntity(digestInputStream, file.length()));
                                CloseableHttpResponse putResponse = httpclient.execute(httpput);
                                try {
                                    int putStatus = putResponse.getStatusLine().getStatusCode();
                                    String putRes = null;
                                    HttpEntity putEntity = putResponse.getEntity();
                                    if (putEntity != null) {
                                        putRes = EntityUtils.toString(putEntity);
                                    }
                                    if (putStatus == 200) {
                                        String localchecksum = Hex.encodeHexString(digestInputStream.getMessageDigest().digest());
                                        if (singleFile) {
                                            dataId = registerFileWithDataverse(file, path, storageIdentifier, localchecksum, retries);
                                        } else {
                                            JSONObject jsonData = new JSONObject();
                                            jsonData.put("storageIdentifier", storageIdentifier);
                                            jsonData.put("fileName", file.getName());
                                            jsonData.put("mimeType", file.getMimeType());
                                            jsonData.put("md5Hash", localchecksum);
                                            jsonData.put("fileSize", file.length());
                                            if (recurse) {
                                                // Dataverse takes paths without an initial / and ending without a /
                                                // with the path not including the file name
                                                if (path.substring(1).contains("/")) {
                                                    String parentPath = path.substring(1, path.lastIndexOf("/"));
                                                    if (!parentPath.isEmpty()) {
                                                        jsonData = jsonData.put("directoryLabel", parentPath);
                                                    }
                                                }
                                            }
                                            file.setMetadata(jsonData);
                                            dataId = "md5:" + localchecksum;
                                        }
                                        if (dataId != null) {
                                            retries = 0;
                                        } else {
                                            println("Failure registering " + file.getName() + " with Dataverse");
                                        }
                                    }
                                } catch (IOException e) {
                                    e.printStackTrace(System.out);
                                    println("Error processing POST to Dataverse" + file.getAbsolutePath() + " : " + e.getMessage());
                                    retries = 0;
                                }
                            }

                        } catch (NoSuchAlgorithmException nsae) {
                            println("MD5 algorithm not found: " + nsae.getMessage());
                        }
                    } else {

                        String abortUrl = uploadResponse.getString("abort");
                        String completeUrl = uploadResponse.getString("complete");
                        JSONObject uploadUrls = uploadResponse.getJSONObject("urls");

                        //And we're sent the storageIdentifier - not strictly needed since the storageIdentifier is already in all the URLS where it's needed.
                        String storageIdentifier = uploadResponse.getString("storageIdentifier");

                        //Queue up all parts so that each part can be uploaded via a different thread.
                        //Using httpConcurrency number of threads to match the number of open HTTP calls to the S3 server that we're allowed in the HttpClient pool
                        ExecutorService executor = Executors.newFixedThreadPool(httpConcurrency);

                        //Give the HttpPartUploadJob class the common info it needs to send parts to S3
                        HttpPartUploadJob.setHttpClient(getSharedHttpClient());
                        HttpPartUploadJob.setHttpClientContext(getLocalContext());
                        HttpPartUploadJob.setPartSize(maxPartSize);

                        //Create a map to store the eTags from the parts and the md5 calculated for the whole file
                        Map<String, String> mpUploadInfoMap = new HashMap<String, String>(uploadUrls.length() + 1);
                        //Setup a job to calculate the md5 hash of the file
                        //Probably helpful to have it run in parallel, but it could be a pre or post step as well. If the network is fast relative to disk, we may want the executor to use one extra thread for this
                        MD5Job mjob = new MD5Job(file, mpUploadInfoMap);
                        executor.execute(mjob);

                        //Now set up upload jobs for each part
                        int i = 1;
                        long remainingSize = file.length();
                        while (uploadUrls.has(Integer.toString(i))) {
                            //Calculate part size
                            long partSize = maxPartSize;
                            if (remainingSize < maxPartSize) {
                                partSize = remainingSize;
                            }
                            remainingSize -= partSize;
                            println("Creating job for " + partSize + " bytes");
                            /*The .replace("%3B",";") in the next line is a work-around for Dell's Isolon storage S3 implementation which can't handle the endcoded char
                              in the X-Amz-SignedHeaders param. Currently this is the only place the encoded ; is present and we've confirmed that unencoding also works with Amazon's S3 stores.
                              If there are ever issues, we could create a -dell flag, only apply this to the specific param involved, etc. to drop this work-around when not needed.
                            */
                            HttpPartUploadJob uj = new HttpPartUploadJob(i, uploadUrls.getString(Integer.toString(i)).replace("%3B",";"), file, partSize, mpUploadInfoMap);

                            executor.execute(uj);
                            i++;
                        }
                        float total = (float) (i - 1);
                        println("All " + total + " parts for: " + storageIdentifier + " queued.");
                        //Tell the executor that there are no more jobs coming
                        executor.shutdown();
                        //And wait until it finishes the ones that are running
                        try {
                            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                                printStatus(mpUploadInfoMap.size() / total);
                            }
                        } catch (InterruptedException e) {
                            println("Upload of " + storageIdentifier + " interrupted: " + e.getMessage());
                            //Call abort?
                        }
                        boolean fileUploadComplete = true;

                        for (String part : (Set<String>) uploadUrls.keySet()) {
                            if (!mpUploadInfoMap.containsKey(part)) {
                                fileUploadComplete = false;
                                break;
                            }
                        }
                        //Technically, the uploads to S3 could succeed and only the md5 fails, but in this case we still want to abort the MP Upload, not complete it.
                        if (!mpUploadInfoMap.containsKey("md5")) {
                            fileUploadComplete = false;
                        }
                        if (fileUploadComplete) {
                            println("Part uploads Completed for " + storageIdentifier);
                            HttpPut completeUpload = new HttpPut(server + completeUrl + "&key=" + apiKey);
                            JSONObject eTags = new JSONObject();
                            ((Set<String>) mpUploadInfoMap.keySet()).stream().filter(partNo -> (!partNo.equals("md5"))).forEachOrdered(partNo -> {
                                eTags.put(partNo, mpUploadInfoMap.get(partNo));
                            });
                            StringEntity body = new StringEntity(eTags.toString());
                            println("ETags: " + eTags.toString());
                            completeUpload.setEntity(body);
                            completeUpload.setHeader("Content-type", "application/json");

                            response = httpclient.execute(completeUpload, getLocalContext());
                            EntityUtils.consumeQuietly(response.getEntity());
                            status = response.getStatusLine().getStatusCode();
                            if (status == 200) {
                                println("Successful upload of " + file.getAbsolutePath());
                                if (singleFile) {
                                    dataId = registerFileWithDataverse(file, path, storageIdentifier, mpUploadInfoMap.get("md5"), retries);
                                } else {
                                    JSONObject jsonData = new JSONObject();
                                    jsonData.put("storageIdentifier", storageIdentifier);
                                    jsonData.put("fileName", file.getName());
                                    jsonData.put("mimeType", file.getMimeType());
                                    jsonData.put("md5Hash", mpUploadInfoMap.get("md5"));
                                    jsonData.put("fileSize", file.length());
                                    if (recurse) {
                                        // Dataverse takes paths without an initial / and ending without a /
                                        // with the path not including the file name
                                        if (path.substring(1).contains("/")) {
                                            String parentPath = path.substring(1, path.lastIndexOf("/"));
                                            if (!parentPath.isEmpty()) {
                                                jsonData = jsonData.put("directoryLabel", parentPath);
                                            }
                                        }
                                    }
                                    file.setMetadata(jsonData);
                                    dataId = "md5:" + mpUploadInfoMap.get("md5");
                                }
                            } else {
                                println("Partial upload of " + file.getAbsolutePath() + ", complete upload failed with status: " + status);
                            }

                            retries = 0;
                        } else {
                            HttpDelete delete = new HttpDelete(server + abortUrl + "&key=" + apiKey);
                            response = httpclient.execute(delete, getLocalContext());
                            EntityUtils.consumeQuietly(response.getEntity());
                            status = response.getStatusLine().getStatusCode();
                            if (status != 204) {
                                println("Call to " + abortUrl + " failed with status: " + status);
                                retries = 0;
                            } else {
                                println("Upload of " + file.getAbsolutePath() + " failed and upload request successfully aborted.");
                                println("Upload of large files is not automatically retried - run again to retry this file upload.");
                                retries = 0;
                            }
                        }
                    }
                } else {
                    println("Retrying request for file upload URL(s): return status was : " + status);
                    if(status == 404) {
                        println("Direct Uploads are not enabled for this dataset. You should contact the Dataverse administrator or, for smaller files, consider using the less efficient -uploadviaserver flag.");
                        retries = 0;
                    } else {
                        retries--;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace(System.out);
                println("Error processing file upload " + file.getAbsolutePath() + " : " + e.getMessage());
                retries = 0;
            }
        }
        return dataId;
    }

    private String registerFileWithDataverse(Resource file, String path, String storageIdentifier, String checksum, int retries) {
        String dataId = null;
        // Now post data
        String urlString = server + "/api/datasets/:persistentId/add";
        urlString = urlString + "?persistentId=" + datasetPID + "&key=" + apiKey;
        while (retries > 0) {
            HttpPost httppost = new HttpPost(urlString);

            // ContentBody bin = file.getContentBody();
            MultipartEntityBuilder meb = MultipartEntityBuilder.create();

            JSONObject jsonData = new JSONObject();
            jsonData.put("storageIdentifier", storageIdentifier);
            jsonData.put("fileName", file.getName());
            jsonData.put("mimeType", file.getMimeType());
            jsonData.put("md5Hash", checksum);
            jsonData.put("fileSize", file.length());
            if (recurse) {
                // Dataverse takes paths without an initial / and ending without a /
                // with the path not including the file name
                if (path.substring(1).contains("/")) {
                    String parentPath = path.substring(1, path.lastIndexOf("/"));
                    if (!parentPath.isEmpty()) {
                        jsonData = jsonData.put("directoryLabel", parentPath);
                    }
                }
            }
            meb.addTextBody("jsonData", jsonData.toString());

            HttpEntity reqEntity = meb.build();
            httppost.setEntity(reqEntity);
            try {
                CloseableHttpResponse postResponse = httpclient.execute(httppost, getLocalContext());

                int postStatus = postResponse.getStatusLine().getStatusCode();
                String postRes = null;
                HttpEntity postEntity = postResponse.getEntity();
                if (postEntity != null) {
                    postRes = EntityUtils.toString(postEntity);
                }
                if (postStatus == 200) {
                    JSONObject checksumObject = (new JSONObject(postRes)).getJSONObject("data")
                            .getJSONArray("files").getJSONObject(0).getJSONObject("dataFile")
                            .getJSONObject("checksum");
                    dataId = checksumObject.getString("type") + ":" + checksumObject.getString("value");
                    retries = 0;
                    int total = 0;
                    // For new servers, wait up to maxWaitTime for a dataset lock to expire.
                    while (isLocked() && (total < maxWaitTime)) {
                        TimeUnit.SECONDS.sleep(1);
                        total = total + 1;
                    }
                } else if (postStatus == 400 && oldServer) {
                    // If the call to the lock API fails in isLocked(), oldServer will be set to
                    // true and
                    // all we can do for a lock is to keep retrying.
                    // Unfortunately, the error messages are configurable, so there's no guaranteed
                    // way to detect
                    // locks versus other conditions (e.g. file exists), so we can test for unique
                    // words in the default messages
                    if ((postRes != null) && postRes.contains("lock")) {
                        retries--;
                    } else {
                        println("Error response when processing " + file.getAbsolutePath() + " : "
                                + postResponse.getStatusLine().getReasonPhrase());
                        // A real error: e.g. This file already exists in the dataset.
                        if (postRes != null) {
                            println(postRes);
                        }
                        // Skip
                        retries = 0;
                    }
                } else {
                    // An error and unlikely that we can recover, so report and move on.
                    println("Error response when processing " + file.getAbsolutePath() + " : "
                            + postResponse.getStatusLine().getReasonPhrase());
                    if (postRes != null) {
                        println(postRes);
                    }
                    retries = 0;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                retries = 0;
            } catch (IOException ex) {
                retries = 0;
                println("Error registering file with dataverse: " + storageIdentifier + " : " + ex.getMessage());
            }
        }
        return dataId;
    }

    private void initHttpPool() throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException {
        if (trustCerts) {
            // use the TrustSelfSignedStrategy to allow Self Signed Certificates
            SSLContext sslContext;
            SSLConnectionSocketFactory connectionFactory;

            sslContext = SSLContextBuilder
                    .create()
                    .loadTrustMaterial(new TrustAllStrategy())
                    .build();
            // create an SSL Socket Factory to use the SSLContext with the trust self signed certificate strategy
            // and allow all hosts verifier.
            connectionFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);

            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("https", connectionFactory).build();
            cm = new PoolingHttpClientConnectionManager(registry);
        } else {
            cm = new PoolingHttpClientConnectionManager();
        }
        cm.setDefaultMaxPerRoute(httpConcurrency);
        cm.setMaxTotal(httpConcurrency > 20 ? httpConcurrency : 20);
    }



    private void addDatasetMetadata(Resource dir) {
      dir.getMetadata();
    }
}
