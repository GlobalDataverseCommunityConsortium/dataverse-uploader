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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.sead.uploader.util.PublishedFolderProxyResource;
import org.sead.uploader.util.PublishedResource;
import org.sead.uploader.util.Resource;
import org.sead.uploader.util.ResourceFactory;

import org.sead.uploader.AbstractUploader;
import org.sead.uploader.util.UploaderException;

/**
 * The SEAD Uploader supports the upload of files/directories from a local disk,
 * or existing SEAD publications for which a valid OREMap file is available from
 * a URL (repositories must update the data file links in the ORE for the
 * Uploader to retrieve them)
 *
 * In addition to sending files and creating a SEAD collection/dataset (1.5) or
 * Dataset/Folder/File (2.0) structure, the Uploader adds path metadata, usable
 * in detecting whether an item has already been created/uploaded. For
 * publications, it also sends metadata, tags, comments, and spatial reference
 * metadata, performing some mapping to clarify when metadata applies only to
 * the original/published version and when the new live copy 'inherits' the
 * metadata. This can be adjusted using the black and gray lists of terms and/or
 * providing custom code to map metadata to SEAD 2.0 conventions.
 *
 */
public class SEADUploader extends AbstractUploader {

    public static final String FRBR_EO = "http://purl.org/vocab/frbr/core#embodimentOf";
    private static final String DCTERMS_HAS_PART = "http://purl.org/dc/terms/hasPart";

    private static boolean d2a = false;
    private static String apiKey = null;
    private static String knownId = null;
    private static boolean checkDataset = false;
    private static String sead2datasetId = null;
    private static int numFoundDatasets = 0;

    private static String CLOWDER_DEFAULT_VOCAB = "https://clowder.ncsa.illinois.edu/contexts/dummy";

    public static void main(String args[]) throws Exception {
        setUploader(new SEADUploader());
        uploader.createLogFile("SEADUploaderLog_");
        uploader.setSpaceType("SEAD2");
        println("\n----------------------------------------------------------------------------------\n");
        println("SSS   EEEE    A    DDD");
        println("S     E      A A   D  D");
        println(" SS   EEE   AAAAA  D   D");
        println("   S  E     A   A  D  D");
        println("SSS   EEEE  A   A  DDD");

        println("SEADUploader - a command-line application to upload files to any Clowder Dataset");
        println("Developed for the SEAD (https://sead-data.net) Community");
        println("\n----------------------------------------------------------------------------------\n");
        println("\n***Parsing arguments:***\n");
        uploader.parseArgs(args);

        if (server == null || requests.isEmpty()) {
            println("\n***Required arguments not found.***");
            usage();
        } else if(checkDataset) {
          ((SEADUploader)uploader).checkTheDataset();
          
        } else {
            println("\n***Starting to Process Upload Requests:***\n");
            uploader.processRequests();
        }
        println("\n***Execution Complete.***");
    }

    private static void usage() {
        println("\nUsage:");
        println("  java -cp .;sead2.1.jar org.sead.uploader.clowder.SEADUploader -server=<serverURL> <files or directories>");

        println("\n  where:");
        println("      <serverUrl> = the URL of the server to upload to, e.g. https://sead2.ncsa.illinois.edu");
        println("      <directories> = a space separated list of directory name(s) to upload as Dataset(s) containing the folders and files within them");
        println("\n  Optional Arguments:");
        println("      -key=<apiKey> - your personal apikey, created in the server at <serverUrl>");
        println("                    - using an apiKey avoids having to enter your username/password and having to reauthenticate for long upload runs");
        println("      -id=<id>      - if you know a dataset exists, specifying it's id will improve performance");
        println("      -listonly     - Scan the Dataset and local files and list what would be uploaded (does not upload with this flag)");
        println("      -limit=<n>    - Specify a maximum number of files to upload per invocation.");
        println("      -verify       - Check both the file name and checksum in comparing with current Dataset entries.");
        println("      -skip=<n>     - a number of files to skip before starting processing (saves time when you know the first n files have been uploaded before)");
        println("      -forcenew     - A new dataset will be created for this upload, even if a matching one is found.");
        println("      -importRO     - uploads from a zipped BagIt file rather than from disk");
        println("");

    }

    private void checkTheDataset() {
        if (knownId == null) {
            println("CheckId only works with knownId - exiting");
            System.exit(0);
        }
        int goodFiles=0;
        String dPath = null;
        CloseableHttpResponse response = null;
        String serviceUrl = "";
        try {
            CloseableHttpClient httpclient = getSharedHttpClient();

            serviceUrl = server + "/api/datasets/" + knownId
                    + "/metadata.jsonld";
            println(serviceUrl);

            HttpGet httpget = new HttpGet(appendKeyIfUsed(serviceUrl));
            response = httpclient.execute(httpget,
                    getLocalContext());
            try {
                if (response.getStatusLine()
                        .getStatusCode() == 200) {
                    HttpEntity resEntity = response
                            .getEntity();
                    if (resEntity != null) {
                        JSONArray mdList = new JSONArray(
                                EntityUtils
                                        .toString(resEntity));

                        for (int j = 0; j < mdList.length(); j++) {
                            if (mdList
                                    .getJSONObject(j)
                                    .getJSONObject(
                                            "content")
                                    .has("Upload Path")) {
                                dPath = mdList
                                        .getJSONObject(
                                                j)
                                        .getJSONObject(
                                                "content")
                                        .getString(
                                                "Upload Path")
                                        .trim();
                                existingDatasets
                                        .put(dPath, knownId);
                                break;
                            }
                        }
                    }
                } else {
                    println("Error response when getting metadata for dataset: "
                            + knownId
                            + " : "
                            + response.getStatusLine()
                                    .getReasonPhrase());
                    println("Exiting. Please contact SEAD about the error.");
                    System.exit(1);

                }
            } finally {
                try {
                    response.close();
                } catch (IOException ex) {
                    Logger.getLogger(SEADUploader.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (dPath == null) {
                println("Dataset: " + knownId + " does not have an Upload Path");
                System.exit(0);
            }

            
            existingFiles = new HashMap<String, String>();
            existingFolders = new HashMap<String, String>();
            try {
                try {
                    serviceUrl = server + "/api/datasets/"
                            + knownId + "/folders";
                    httpget = new HttpGet(appendKeyIfUsed(serviceUrl));
                    response = httpclient.execute(
                            httpget, getLocalContext());
                    if (response.getStatusLine().getStatusCode() == 200) {
                        HttpEntity resEntity = response.getEntity();
                        if (resEntity != null) {
                            JSONArray folders = new JSONArray(
                                    EntityUtils.toString(resEntity));
                            for (int i = 0; i < folders.length(); i++) {
                                existingFolders.put(folders.getJSONObject(i)
                                        .getString("name"), folders.getJSONObject(i)
                                        .getString("id"));
                            }
                        }
                    } else {
                        println("Error response when checking for folders"
                                + " : "
                                + response.getStatusLine()
                                        .getReasonPhrase());
                        println("Exiting. Please contact SEAD about the error.");
                        System.exit(1);

                    }
                } finally {
                    response.close();
                }
            } catch (IOException e) {
                println("Error processing folders: " + e.getMessage());
            } finally {
                folderMDRetrieved = true;
            }

            try {
                serviceUrl = server + "/api/datasets/"
                        + knownId + "/listAllFiles";

                httpget = new HttpGet(appendKeyIfUsed(serviceUrl));

                response = httpclient.execute(
                        httpget, getLocalContext());
                JSONArray fileList = null;
                try {
                    if (response.getStatusLine().getStatusCode() == 200) {
                        HttpEntity resEntity = response.getEntity();
                        if (resEntity != null) {
                            fileList = new JSONArray(
                                    EntityUtils.toString(resEntity));
                        }
                    } else {
                        println("Error response when checking files"
                                + " : "
                                + response.getStatusLine()
                                        .getReasonPhrase());
                        println("Exiting. Please contact SEAD about the error.");
                        System.exit(1);

                    }
                } finally {
                    response.close();
                }
                if (fileList != null) {
                    for (int i = 0; i < fileList.length(); i++) {
                        String id = fileList.getJSONObject(i)
                                .getString("id");
                        serviceUrl = server + "/api/files/" + id
                                + "/metadata.jsonld";

                        httpget = new HttpGet(appendKeyIfUsed(serviceUrl));

                        response = httpclient.execute(httpget,
                                getLocalContext());

                        try {
                            if (response.getStatusLine()
                                    .getStatusCode() == 200) {
                                HttpEntity resEntity = response
                                        .getEntity();
                                if (resEntity != null) {
                                    JSONArray mdList = new JSONArray(
                                            EntityUtils
                                                    .toString(resEntity));
                                    boolean hasPath = false;
                                    for (int j = 0; j < mdList.length(); j++) {
                                        if (mdList
                                                .getJSONObject(j)
                                                .getJSONObject(
                                                        "content")
                                                .has("Upload Path")) {
                                            String path = mdList
                                                    .getJSONObject(
                                                            j)
                                                    .getJSONObject(
                                                            "content")
                                                    .getString(
                                                            "Upload Path")
                                                    .trim();
                                            if (existingFiles.containsKey(path)) {
                                                println(id + " duplicates " + existingFiles.get(path));
                                            } else {
                                                String folderPath = path.substring(1,path.lastIndexOf("/"));
                                                if(folderPath.indexOf("/")>=0) {
                                                folderPath = folderPath.substring(folderPath.indexOf("/"));
                                                } else {
                                                    folderPath="";
                                                }
                                                if (folderPath.length()>0 && !existingFolders.containsKey(folderPath)) {
                                                    println("Folder for " + path + " not found");
                                                } else {

                                                    existingFiles
                                                            .put(path, id);
                                                    goodFiles++;
                                                    if (goodFiles % 10 == 0) {
                                                        System.out.print(".");
                                                    }
                                                    if (goodFiles % 1000 == 0) {
                                                        System.out.print("Processed " + goodFiles + " good files.");
                                                    }

                                                }
                                            }
                                            hasPath = true;
                                            break;
                                        }
                                    }
                                    if (!hasPath) {
                                        println("File with no path: " + id);
                                    }
                                }

                            } else {
                                println("Error response when getting metadata for file: "
                                        + id
                                        + " : "
                                        + response.getStatusLine()
                                                .getReasonPhrase());
                                println("Exiting. Please contact SEAD about the error.");
                                System.exit(1);
                            }
                        } finally {
                            response.close();
                        }
                    }

                }
            } finally {
                response.close();
                httpclient.close();
            }
        } catch (IOException io) {
            println("Error doing " + serviceUrl + " : " + io.getMessage());
        }
        println("Analysis complete with " + goodFiles + " good files.");
    }

    public boolean parseCustomArg(String arg) {
        if (arg.equalsIgnoreCase("-d2a")) {
            d2a = true;
            println("Description to Abstract translation on");
            return true;
        } else if (arg.startsWith("-key")) {
            apiKey = arg.substring(arg.indexOf(argSeparator) + 1);
            println("Using apiKey: " + apiKey);
            return true;
        } else if (arg.startsWith("-id")) {
            knownId = arg.substring(arg.indexOf(argSeparator) + 1);
            println("Updating Dataset with id: " + knownId);
            return true;
        } else if(arg.startsWith("-checkDataset")) {
            checkDataset=true;
            println("Only checking Dataset");
            return true;
        }
        return false;
    }

    CloseableHttpClient httpClient = null;

    public CloseableHttpClient getSharedHttpClient() {
        if (httpClient == null) {
            // use the TrustSelfSignedStrategy to allow Self Signed Certificates
            SSLContext sslContext;
            try {
                sslContext = SSLContextBuilder
                        .create()
                        .loadTrustMaterial(new TrustAllStrategy())
                        .build();

                // create an SSL Socket Factory to use the SSLContext with the trust self signed certificate strategy
                // and allow all hosts verifier.
                SSLConnectionSocketFactory connectionFactory = new SSLConnectionSocketFactory(sslContext);

                // finally create the HttpClient using HttpClient factory methods and assign the ssl socket factory
                httpClient = HttpClients
                        .custom()
                        .setSSLSocketFactory(connectionFactory)
                        .build();
            } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException ex) {
                Logger.getLogger(SEADUploader.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return httpClient;
    }

    @Override
    public void processRequests() {
        println("Contacting server...");
        getSharedHttpClient();
        super.processRequests();
        println("Closing server connection...");

        try {
            getSharedHttpClient().close();
        } catch (IOException ex) {
            Logger.getLogger(SEADUploader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public HttpClientContext authenticate() {
        if (apiKey != null) {
            //Don't need to update context since we have the apikey to use
            if (getLocalContext() == null) {
                return (new HttpClientContext());
            } else {
                return getLocalContext();
            }
        }
        return SEADAuthenticator
                .UPAuthenticate(server);
    }

    private void moveFileToFolder(String newUri, String parentId,
            Resource file) {
        CloseableHttpClient httpclient = getSharedHttpClient();
        try {
            HttpPost httppost = new HttpPost(appendKeyIfUsed(server + "/api/datasets/"
                    + sead2datasetId + "/moveFile/" + parentId + "/" + newUri));

            StringEntity se = new StringEntity("{}");
            se.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,
                    "application/json"));
            httppost.setEntity(se);

            CloseableHttpResponse response = httpclient.execute(httppost,
                    getLocalContext());
            HttpEntity resEntity = null;
            try {
                if (response.getStatusLine().getStatusCode() == 200) {
                    EntityUtils.consume(response.getEntity());
                } else {
                    println("Error response when processing "
                            + file.getAbsolutePath() + " : "
                            + response.getStatusLine().getReasonPhrase());
                    println("Details: "
                            + EntityUtils.toString(response.getEntity()));
                }
            } finally {
                EntityUtils.consumeQuietly(resEntity);
                response.close();
            }

            // FixMe Add tags
            /*
			 * if (tagValues != null) { addTags(httpclient, dir, collectionId,
			 * tagValues); }
             */
        } catch (IOException e) {
            println("Error processing " + file.getAbsolutePath() + " : "
                    + e.getMessage());
        }
    }

    private String create2Folder(String parentId, String sead2datasetId,
            String path, Resource dir) {
        String collectionId = null;
        CloseableHttpClient httpclient = getSharedHttpClient();
        try {
            String postUri = server + "/api/datasets/"
                    + sead2datasetId + "/newFolder";
            if (apiKey != null) {
                postUri = postUri + "?key=" + apiKey;
            }
            HttpPost httppost = new HttpPost(appendKeyIfUsed(server + "/api/datasets/"
                    + sead2datasetId + "/newFolder"));

            JSONObject jo = new JSONObject();
            String title = dir.getName().trim();
            // For publishedResource, we want to use the Title
            if (dir instanceof PublishedResource) {
                title = ((PublishedResource) dir).getAndRemoveTitle().trim();
            }
            jo.put("name", title);
            jo.put("parentId", parentId);
            jo.put("parentType", ((parentId == sead2datasetId) ? "dataset"
                    : "folder"));

            StringEntity se = new StringEntity(jo.toString(), "UTF-8");
            se.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,
                    "application/json; charset=utf-8"));
            httppost.setEntity(se);

            CloseableHttpResponse response = httpclient.execute(httppost,
                    getLocalContext());
            HttpEntity resEntity = null;
            try {
                if (response.getStatusLine().getStatusCode() == 200) {
                    EntityUtils.consume(response.getEntity());
                    // Now query to get the new folder's id
                    // path should be of the form
                    // "/<datasetname>/[<parentfolder(s)>/]<thisfolder>" for
                    // file uploads and
                    // "<ro_id>/data/<datasetname>/[<parentfolder(s)>/]<thisfolder>"
                    // for imported ROs
                    // and we need to strip to get only the folder path part
                    String folderPath = path;
                    if (importRO) {
                        folderPath = folderPath.substring(folderPath.substring(
                                1).indexOf("/") + 1);
                        folderPath = folderPath.substring(folderPath.substring(
                                1).indexOf("/") + 1);
                    }
                    folderPath = folderPath.substring(folderPath.substring(1)
                            .indexOf("/") + 1);

                    HttpGet httpget = new HttpGet(appendKeyIfUsed(server + "/api/datasets/"
                            + sead2datasetId + "/folders"));

                    CloseableHttpResponse getResponse = httpclient.execute(
                            httpget, getLocalContext());
                    try {
                        if (getResponse.getStatusLine().getStatusCode() == 200) {
                            JSONArray folders = new JSONArray(
                                    EntityUtils.toString(getResponse
                                            .getEntity()));
                            for (int i = 0; i < folders.length(); i++) {
                                if (folders.getJSONObject(i).getString("name")
                                        .equals(folderPath)) {
                                    collectionId = folders.getJSONObject(i)
                                            .getString("id");
                                    break;
                                }
                            }
                        } else {
                            println("Error response when processing "
                                    + dir.getAbsolutePath()
                                    + " : "
                                    + getResponse.getStatusLine()
                                            .getReasonPhrase());
                            println("Details: "
                                    + EntityUtils.toString(getResponse
                                            .getEntity()));
                        }
                    } finally {
                        EntityUtils.consumeQuietly(getResponse.getEntity());
                        getResponse.close();
                    }
                } else {
                    println("Error response when processing "
                            + dir.getAbsolutePath() + " : "
                            + response.getStatusLine().getReasonPhrase());
                    println("Details: "
                            + EntityUtils.toString(response.getEntity()));
                }
            } finally {
                EntityUtils.consumeQuietly(resEntity);
                response.close();
            }

            // Add metadata for imported folders
            // FixMe - Add Metadata to folder directly
            // Assume we only write a metadata file if collection is newly
            // created and we're importing
            if (importRO && collectionId != null) {
                Resource mdFile = new PublishedFolderProxyResource(
                        (PublishedResource) dir, collectionId);
                String mdId = null;
                try {
                    mdId = uploadDatafile(mdFile, path + "/"
                            + mdFile.getName());
                } catch (UploaderException ue) {
                    println(ue.getMessage());
                }
                // By default, we are in a folder and need to move the file
                // (sead2datasetId != collectionId))
                if (mdId != null) { // and it was just
                    // created
                    moveFileToFolder(mdId, collectionId, mdFile);
                    roFolderProxy.put(collectionId, mdId);
                } else {
                    println("Unable to write metadata file for folder: "
                            + collectionId);
                }
            }
        } catch (IOException e) {
            println("Error processing " + dir.getAbsolutePath() + " : "
                    + e.getMessage());
        }
        return collectionId;

    }

    private String create2Dataset(Resource dir, String path) {
        String datasetId = null;
        CloseableHttpClient httpclient = getSharedHttpClient();
        try {

            HttpPost httppost = new HttpPost(appendKeyIfUsed(server
                    + "/api/datasets/createempty"));
            JSONObject jo = new JSONObject();
            String title = dir.getName().trim();
            // For publishedResource, we want to use the Title
            if (dir instanceof PublishedResource) {
                title = ((PublishedResource) dir).getAndRemoveTitle().trim();
            }
            jo.put("name", title);
            if (importRO) {
                String abs = ((PublishedResource) dir)
                        .getAndRemoveAbstract(d2a);
                if (abs != null) {
                    jo.put("description", abs);
                }
            }

            StringEntity se = new StringEntity(jo.toString(), "UTF-8");
            se.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,
                    "application/json; charset=utf-8"));
            httppost.setEntity(se);

            CloseableHttpResponse response = httpclient.execute(httppost,
                    getLocalContext());
            HttpEntity resEntity = null;
            try {
                resEntity = response.getEntity();
                if (response.getStatusLine().getStatusCode() == 200) {
                    if (resEntity != null) {
                        datasetId = new JSONObject(
                                EntityUtils.toString(resEntity))
                                .getString("id");
                    }
                } else {
                    println("Error response when processing "
                            + dir.getAbsolutePath() + " : "
                            + response.getStatusLine().getReasonPhrase());
                    println("Details: " + EntityUtils.toString(resEntity));
                }
            } finally {
                EntityUtils.consumeQuietly(resEntity);
                response.close();
            }
            if (datasetId != null) {

                // Add Metadata
                JSONObject content = new JSONObject();
                JSONObject context = new JSONObject();
                JSONObject agent = new JSONObject();
                List<String> creators = new ArrayList<String>();
                content.put("Upload Path", path);
                List<String> comments = new ArrayList<String>();
                // Should be true for all PublishedResources, never for files...
                if (dir instanceof PublishedResource) {
                    ((PublishedResource) dir).getAndRemoveCreator(creators);
                }

                String creatorPostUri = server + "/api/datasets/" + datasetId
                        + "/creator";
                for (String creator : creators) {
                    postDatasetCreator(creator, creatorPostUri, httpclient);
                }

                String tagValues = add2ResourceMetadata(content, context,
                        agent, comments, path, dir);

                postMetadata(httpclient, server + "/api/datasets/" + datasetId
                        + "/metadata.jsonld", dir.getAbsolutePath(), content,
                        context, agent);
                if (creators != null) {

                }
                // FixMe Add tags
                if (tagValues != null) {
                    HttpPost tagPost = new HttpPost(appendKeyIfUsed(server + "/api/datasets/"
                            + datasetId + "/tags"));
                    JSONObject tags = new JSONObject();

                    String[] tagArray = tagValues.split(",");
                    JSONArray tagList = new JSONArray(tagArray);
                    tags.put("tags", tagList);

                    StringEntity se3 = new StringEntity(tags.toString(),
                            "UTF-8");
                    se3.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,
                            "application/json; charset=utf-8"));
                    tagPost.setEntity(se3);

                    CloseableHttpResponse tagResponse = httpclient.execute(
                            tagPost, getLocalContext());
                    resEntity = null;
                    try {
                        resEntity = tagResponse.getEntity();
                        if (tagResponse.getStatusLine().getStatusCode() != 200) {
                            println("Error response when processing "
                                    + dir.getAbsolutePath()
                                    + " : "
                                    + tagResponse.getStatusLine()
                                            .getReasonPhrase());
                            println("Details: "
                                    + EntityUtils.toString(resEntity));
                        }
                    } finally {
                        EntityUtils.consumeQuietly(resEntity);
                        tagResponse.close();
                    }
                }
                if (comments.size() > 0) {
                    Collections.sort(comments);
                    for (String text : comments.toArray(new String[comments
                            .size()])) {
                        HttpPost commentPost = new HttpPost(appendKeyIfUsed(server
                                + "/api/datasets/" + datasetId + "/comment"));

                        JSONObject comment = new JSONObject();
                        comment.put("text", text);

                        StringEntity se3 = new StringEntity(comment.toString(),
                                "UTF-8");
                        se3.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,
                                "application/json; charset=utf-8"));
                        commentPost.setEntity(se3);

                        CloseableHttpResponse commentResponse = httpclient
                                .execute(commentPost, getLocalContext());
                        resEntity = null;
                        try {
                            resEntity = commentResponse.getEntity();
                            if (commentResponse.getStatusLine().getStatusCode() != 200) {
                                println("Error response when processing "
                                        + dir.getAbsolutePath()
                                        + " : "
                                        + commentResponse.getStatusLine()
                                                .getReasonPhrase());
                                println("Details: "
                                        + EntityUtils.toString(resEntity));
                            }
                        } finally {
                            EntityUtils.consumeQuietly(resEntity);
                            commentResponse.close();
                        }
                    }
                }
            }
        } catch (IOException e) {
            println("Error processing " + dir.getAbsolutePath() + " : "
                    + e.getMessage());
        }
        return datasetId;
    }

    @SuppressWarnings("unchecked")
    private String add2ResourceMetadata(JSONObject content,
            JSONObject context, JSONObject agent, List<String> comments,
            String path, Resource item) {
        Object tags = null;

        JSONObject metadata = item.getMetadata(); // Empty for file resources
        if (metadata.has("Metadata on Original")) {
            JSONObject original = metadata
                    .getJSONObject("Metadata on Original");
            // Gray list metadata should be used (and removed) from
            // this
            // field or passed in as is

            if (original.has("Keyword")) {
                tags = original.get("Keyword");
                original.remove("Keyword");
            }
            if (original.has("GeoPoint")) {
                Object gpObject = original.get("GeoPoint");
                if (gpObject instanceof JSONArray) {
                    // An error so we'll just capture as a string
                    metadata.put("Geolocation",
                            "This entry had multiple Lat/Long from SEAD 1.5 GeoPoints: "
                            + ((JSONArray) gpObject).toString(2));
                } else {
                    JSONObject point = original.getJSONObject("GeoPoint");
                    metadata.put("Geolocation",
                            "Lat/Long from SEAD 1.5 GeoPoint");
                    metadata.put("Latitude", point.getString("lat"));
                    metadata.put("Longitude", point.getString("long"));
                    original.remove("GeoPoint");
                }

            }

            if (original.has("Comment")) {
                Object comObject = original.get("Comment");
                if (comObject instanceof JSONArray) {
                    for (int i = 0; i < ((JSONArray) comObject).length(); i++) {
                        JSONObject comment = ((JSONArray) comObject)
                                .getJSONObject(i);
                        comments.add(getComment(comment));
                    }
                } else {
                    comments.add(getComment(((JSONObject) comObject)));
                }
                original.remove("Comment");
            }
        }
        // Convert all vals to Strings
        for (String key : (Set<String>) metadata.keySet()) {
            String newKey = key;
            if (ResourceFactory.graySwaps.containsKey(key)) {
                newKey = ResourceFactory.graySwaps.get(key);
            }
            if (metadata.get(key) instanceof JSONArray) {
                // split values and handle them separately
                JSONArray valArray = (JSONArray) metadata.get(key);
                JSONArray newVals = new JSONArray();
                for (int i = 0; i < valArray.length(); i++) {
                    String val = valArray.get(i).toString();
                    newVals.put(val);
                }
                content.put(newKey, newVals);
            } else {
                content.put(newKey, metadata.get(key).toString());
            }
        }
        // create tag(s) string
        String tagValues = null;
        if (tags != null) {
            if (tags instanceof JSONArray) {
                tagValues = "";
                JSONArray valArray = (JSONArray) tags;
                for (int i = 0; i < valArray.length(); i++) {
                    tagValues = tagValues + valArray.get(i).toString();
                    if (valArray.length() > 1 && i != valArray.length() - 1) {
                        tagValues = tagValues + ",";
                    }
                }
            } else {
                tagValues = ((String) tags);
            }
        }
        content.put("Upload Path", path);

        // Flatten context for 2.0
        context.put("@vocab", CLOWDER_DEFAULT_VOCAB);
        for (String key : ((Set<String>) content.keySet())) {
            if (rf != null) { // importRO == true
                String pred = rf.getURIForContextEntry(key);
                if (pred != null) {
                    context.put(key, pred);
                }
            } else {
                if (key.equals("Upload Path")) {
                    context.put(key, SEADUploader.FRBR_EO);
                } else { // shouldn't happen
                    println("Unrecognized Metadata Entry: " + key);
                }
            }
        }
        JSONObject me = get2me();
        agent.put("name", me.getString("fullName"));
        agent.put("@type", "cat:user");
        agent.put("user_id", server + "/api/users/" + me.getString("id"));

        return tagValues;
    }

    private String getComment(JSONObject comment) {
        StringBuilder sb = new StringBuilder();
        sb.append("Imported Comment: ");
        sb.append(comment.getString("comment_date"));
        sb.append(", Author: ");
        String comAuth = comment.getString("comment_author");
        sb.append(comAuth.substring(comAuth.lastIndexOf("/") + 1));
        sb.append(": ");
        sb.append(comment.getString("comment_body"));
        return sb.toString();
    }

    JSONObject me = null;

    private JSONObject get2me() {
        CloseableHttpClient httpclient = getSharedHttpClient();
        if (me == null) {
            try {
                String serviceUrl = server + "/api/me";

                HttpGet httpget = new HttpGet(appendKeyIfUsed(serviceUrl));
                CloseableHttpResponse response = httpclient.execute(httpget,
                        getLocalContext());
                try {
                    if (response.getStatusLine().getStatusCode() == 200) {
                        HttpEntity resEntity = response.getEntity();
                        if (resEntity != null) {
                            me = new JSONObject(EntityUtils.toString(resEntity));
                        }
                    } else {
                        println("Error response when retrieving user details: "
                                + response.getStatusLine().getReasonPhrase());

                    }
                } finally {
                    response.close();
                }
            } catch (IOException e) {
                println("Error processing get user request: " + e.getMessage());
            }
            // me.put("fullName", "SEAD 1.5 Importer");
        }
        return me;
    }

    @Override
    protected String uploadDatafile(Resource file, String path) throws UploaderException {
        if (sead2datasetId == null) {
            throw new UploaderException("SEAD2 does not support upload of individual files that are not in a dataset.");
        }
        CloseableHttpClient httpclient = getSharedHttpClient();
        String dataId = null;
        try {
            // FixMe: requires update to 2.0 ... To support long uploads,
            // request a key to allow the
            // upload to complete even if the session has timed out

            // Now post data
            String urlString = server + "/api/uploadToDataset/"
                    + sead2datasetId;
            HttpPost httppost = new HttpPost(appendKeyIfUsed(urlString));
            ContentBody bin = file.getContentBody();
            MultipartEntityBuilder meb = MultipartEntityBuilder.create();
            meb.addPart("files[]", bin);

            // FixMe
            // addLiteralMetadata(meb, FRBR_EO, path);
            // FixMe
            // String tagValues = addResourceMetadata(meb, file);
            HttpEntity reqEntity = meb.build();
            httppost.setEntity(reqEntity);

            CloseableHttpResponse response = httpclient.execute(httppost,
                    getLocalContext());
            HttpEntity resEntity = response.getEntity();

            try {
                if (response.getStatusLine().getStatusCode() == 200) {
                    if (resEntity != null) {
                        dataId = new JSONObject(EntityUtils.toString(resEntity))
                                .getString("id");
                    }
                } else {
                    println("Error response when processing "
                            + file.getAbsolutePath() + " : "
                            + response.getStatusLine().getReasonPhrase());
                    println("Details: " + EntityUtils.toString(resEntity));
                }

            } catch (Exception e) {
                println("Error uploading file: " + file.getName());
                e.printStackTrace();
            } finally {
                EntityUtils.consumeQuietly(response.getEntity());
                response.close();
            }
            if (dataId != null) {

                // FixMe - add Metadata
                /*
				 * addLiteralMetadata(meb, FRBR_EO, path);
				 *
				 * // Add metadata for published resources
				 *
				 * String tagValues = addResourceMetadata(meb, dir); HttpEntity
				 * reqEntity = meb.build();
                 */
                JSONObject content = new JSONObject();
                List<String> comments = new ArrayList<String>();
                JSONObject context = new JSONObject();
                JSONObject agent = new JSONObject();

                String abs = null;
                String title = null;
                if (file instanceof PublishedResource) {
                    abs = ((PublishedResource) file).getAndRemoveAbstract(d2a);

                    title = ((PublishedResource) file).getAndRemoveTitle();
                    if ((title != null) && (title.equals(file.getName()))) {
                        title = null;
                    }
                }
                String tagValues = add2ResourceMetadata(content, context,
                        agent, comments, path, file);

                postMetadata(httpclient, server + "/api/files/" + dataId
                        + "/metadata.jsonld", file.getAbsolutePath(), content,
                        context, agent);

                if (abs != null) {
                    HttpPut descPut = new HttpPut(appendKeyIfUsed(server + "/api/files/"
                            + dataId + "/updateDescription"));
                    JSONObject desc = new JSONObject();

                    desc.put("description", abs);

                    StringEntity descSE = new StringEntity(desc.toString(),
                            "UTF-8");
                    descSE.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,
                            "application/json; charset=utf-8"));
                    descPut.setEntity(descSE);

                    CloseableHttpResponse descResponse = httpclient.execute(
                            descPut, getLocalContext());
                    resEntity = null;
                    try {
                        resEntity = descResponse.getEntity();
                        if (descResponse.getStatusLine().getStatusCode() != 200) {
                            println("Error response when processing "
                                    + file.getAbsolutePath()
                                    + " : "
                                    + descResponse.getStatusLine()
                                            .getReasonPhrase());
                            println("Details: "
                                    + EntityUtils.toString(resEntity));
                        }
                    } finally {
                        EntityUtils.consumeQuietly(resEntity);
                        descResponse.close();
                    }
                }
                // We need a valid filename (from "Label"/getName() to do
                // the
                // upload, but, if the user
                // has changed the "Title", we need to then update the
                // displayed
                // filename
                // For folders, this will currently always be null
                // (since Title is used for the name in PublishedResource
                // for directories) and therefore we won't change the name
                // of the readme file
                // as set in the Proxy class.
                if (title != null) {
                    HttpPut namePut = new HttpPut(appendKeyIfUsed(server + "/api/files/"
                            + dataId + "/filename"));
                    JSONObject name = new JSONObject();

                    name.put("name", title);

                    StringEntity nameSE = new StringEntity(name.toString(),
                            "UTF-8");
                    nameSE.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,
                            "application/json; charset=utf-8"));
                    namePut.setEntity(nameSE);

                    CloseableHttpResponse nameResponse = httpclient.execute(
                            namePut, getLocalContext());
                    resEntity = null;
                    try {
                        resEntity = nameResponse.getEntity();
                        if (nameResponse.getStatusLine().getStatusCode() != 200) {
                            println("Error response when processing "
                                    + file.getAbsolutePath()
                                    + " : "
                                    + nameResponse.getStatusLine()
                                            .getReasonPhrase());
                            println("Details: "
                                    + EntityUtils.toString(resEntity));
                        } else {
                            println("Dataset name successfully changed from : "
                                    + file.getName() + " to " + title);
                        }
                    } finally {
                        EntityUtils.consumeQuietly(resEntity);
                        nameResponse.close();
                    }
                }

                // FixMe Add tags
                if (tagValues != null) {
                    HttpPost tagPost = new HttpPost(appendKeyIfUsed(server + "/api/files/"
                            + dataId + "/tags"));
                    JSONObject tags = new JSONObject();

                    String[] tagArray = tagValues.split(",");
                    JSONArray tagList = new JSONArray(tagArray);
                    tags.put("tags", tagList);

                    StringEntity se3 = new StringEntity(tags.toString(),
                            "UTF-8");
                    se3.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,
                            "application/json; charset=utf-8"));
                    tagPost.setEntity(se3);

                    CloseableHttpResponse tagResponse = httpclient.execute(
                            tagPost, getLocalContext());
                    resEntity = null;
                    try {
                        resEntity = tagResponse.getEntity();
                        if (tagResponse.getStatusLine().getStatusCode() != 200) {
                            println("Error response when processing "
                                    + file.getAbsolutePath()
                                    + " : "
                                    + tagResponse.getStatusLine()
                                            .getReasonPhrase());
                            println("Details: "
                                    + EntityUtils.toString(resEntity));
                        }
                    } finally {
                        EntityUtils.consumeQuietly(resEntity);
                        tagResponse.close();
                    }

                }
                if (comments.size() > 0) {
                    Collections.sort(comments);
                    for (String text : comments.toArray(new String[comments
                            .size()])) {
                        HttpPost commentPost = new HttpPost(appendKeyIfUsed(server
                                + "/api/files/" + dataId + "/comment"));

                        JSONObject comment = new JSONObject();
                        comment.put("text", text);

                        StringEntity se4 = new StringEntity(comment.toString(),
                                "UTF-8");
                        se4.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,
                                "application/json; charset=utf-8"));
                        commentPost.setEntity(se4);

                        CloseableHttpResponse commentResponse = httpclient
                                .execute(commentPost, getLocalContext());
                        resEntity = null;
                        try {
                            resEntity = commentResponse.getEntity();
                            if (commentResponse.getStatusLine().getStatusCode() != 200) {
                                println("Error response when processing "
                                        + file.getAbsolutePath()
                                        + " : "
                                        + commentResponse.getStatusLine()
                                                .getReasonPhrase());
                                println("Details: "
                                        + EntityUtils.toString(resEntity));
                            }
                        } finally {
                            EntityUtils.consumeQuietly(resEntity);
                            commentResponse.close();
                        }
                    }
                }

            }
        } catch (IOException e) {
            println("Error processing " + file.getAbsolutePath() + " : "
                    + e.getMessage());
        }
        return dataId;
    }

    @SuppressWarnings("unchecked")
    private void postMetadata(CloseableHttpClient httpclient,
            String uri, String path, JSONObject content, JSONObject context,
            JSONObject agent) {
        Set<String> keys = new HashSet<String>();
        keys.addAll(((Set<String>) content.keySet()));
        if (keys.contains("Geolocation")) {
            keys.remove("Latitude");
            keys.remove("Longitude");
        }

        for (String key : keys) {
            try {
                String safeKey = key.replace(".", "_").replace("$", "_")
                        .replace("/", "_"); // Clowder/MongoDB don't allow keys
                // with .$/ chars

                JSONObject singleContent = new JSONObject().put(safeKey,
                        content.get(key));
                JSONObject singleContext = new JSONObject().put(safeKey,
                        context.get(key));
                // Geolocation stays together with lat and long to mirror
                // how the Clowder GUI works
                if (key.equals("Geolocation")) {
                    if (content.has("Latitude")) {
                        singleContent.put("Latitude", content.get("Latitude"));
                        singleContext.put("Latitude", context.get("Latitude"));
                    }
                    if (content.has("Longitude")) {
                        singleContent
                                .put("Longitude", content.get("Longitude"));
                        singleContext
                                .put("Longitude", context.get("Longitude"));
                    }
                }
                // Clowder expects flat "Creator"s - might as well flatten all
                // values...
                if (singleContent.get(safeKey) instanceof JSONArray) {
                    for (int i = 0; i < ((JSONArray) singleContent
                            .getJSONArray(key)).length(); i++) {
                        JSONObject flatContent = new JSONObject();
                        flatContent.put(key, ((JSONArray) singleContent
                                .getJSONArray(key)).get(i).toString());
                        postSingleMetadata(flatContent, singleContext, agent,
                                uri, httpclient);
                    }
                } else {
                    postSingleMetadata(singleContent, singleContext, agent,
                            uri, httpclient);
                }

            } catch (IOException e) {
                println("Error processing " + path + " : " + e.getMessage());
                break;
            }
        }

    }

    private void postSingleMetadata(JSONObject singleContent,
            JSONObject singleContext, JSONObject agent, String uri,
            CloseableHttpClient httpclient) throws IOException {
        HttpEntity resEntity = null;
        try {
            singleContext.put("@vocab", CLOWDER_DEFAULT_VOCAB);
            JSONObject meta = new JSONObject();
            meta.put("content", singleContent);
            meta.put("@context", singleContext);
            meta.put("agent", agent);

            StringEntity se2 = new StringEntity(meta.toString(), "UTF-8");

            se2.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,
                    "application/json; charset=utf-8"));

            HttpPost metadataPost = new HttpPost(appendKeyIfUsed(uri));

            metadataPost.setEntity(se2);

            CloseableHttpResponse mdResponse = httpclient.execute(metadataPost,
                    getLocalContext());

            resEntity = mdResponse.getEntity();
            if (mdResponse.getStatusLine().getStatusCode() != 200) {
                println("Error response when processing key="
                        + singleContent.keys().next() + " : "
                        + mdResponse.getStatusLine().getReasonPhrase());
                println("Value: "
                        + singleContent.get(
                                singleContent.keys().next().toString())
                                .toString());
                println("Details: " + EntityUtils.toString(resEntity));
                throw new IOException("Non 200 response");
            }
        } finally {
            EntityUtils.consumeQuietly(resEntity);
        }

    }

    private void postDatasetCreator(String creator, String uri,
            CloseableHttpClient httpclient) throws IOException {
        HttpEntity resEntity = null;
        try {
            JSONObject body = new JSONObject();
            body.put("creator", creator);

            StringEntity se2 = new StringEntity(body.toString(), "UTF-8");
            se2.setContentType(new BasicHeader(HTTP.CONTENT_TYPE,
                    "application/json; charset=utf-8"));

            HttpPost creatorPost = new HttpPost(appendKeyIfUsed(uri));

            creatorPost.setEntity(se2);

            CloseableHttpResponse creatorResponse = httpclient.execute(
                    creatorPost, getLocalContext());

            resEntity = creatorResponse.getEntity();
            if (creatorResponse.getStatusLine().getStatusCode() != 200) {
                println("Error response when sending creator: " + creator
                        + " : "
                        + creatorResponse.getStatusLine().getReasonPhrase());
                println("Details: " + EntityUtils.toString(resEntity));
                throw new IOException("Non 200 response");
            }
        } finally {
            EntityUtils.consumeQuietly(resEntity);
        }
    }

    protected String findGeneralizationOf(String id) {
        return id;
    }

    HashMap<String, String> existingDatasets = new HashMap<String, String>();
    HashMap<String, String> existingFolders = new HashMap<String, String>();
    HashMap<String, String> existingFiles = new HashMap<String, String>();

    boolean fileMDRetrieved = false;
    boolean folderMDRetrieved = false;

    public String itemExists(String path, Resource item) {
        String tagId = null;

        String relPath = path;
        if (importRO) {
            // remove the '/<ro_id>/data' prefix on imported paths to make
            // it match the file upload paths
            relPath = relPath
                    .substring(relPath.substring(1).indexOf("/") + 1);
            relPath = relPath
                    .substring(relPath.substring(1).indexOf("/") + 1);
        }
        if (relPath.equals("/")) {
            println("Searching for existing dataset. If this takes a long time, consider using:");
            println("     -id=<id> if you know the dataset exists, or");
            println("     -forcenew if you know the dataset does not yet exist.");

            // It's a dataset
            CloseableHttpClient httpclient = getSharedHttpClient();
            String sourcepath = path + item.getName();
            //If we haven't yet found this dataset because we haven't looked yet, or we looked and haven't yet found this dataset and there are still more to scan...
            if (!existingDatasets.containsKey(sourcepath) && (numFoundDatasets == 0 || existingDatasets.size() < numFoundDatasets)) {
                try {
                    // Only returns first 12 by default
                    String serviceUrl = server + "/api/datasets?limit=0";
                    JSONArray datasetList = null;
                    HttpGet httpget = null;
                    CloseableHttpResponse response = null;
                    if (knownId == null) {
                        //Get the whole list of datasets to scan through
                        httpget = new HttpGet(appendKeyIfUsed(serviceUrl));
                        response = httpclient.execute(
                                httpget, getLocalContext());
                        try {
                            if (response.getStatusLine().getStatusCode() == 200) {
                                HttpEntity resEntity = response.getEntity();
                                if (resEntity != null) {
                                    datasetList = new JSONArray(
                                            EntityUtils.toString(resEntity));
                                    println("Scanning " + datasetList.length() + " datasets for a match...");
                                    numFoundDatasets = datasetList.length();
                                }
                            } else {
                                println("Error response when checking for existing item at "
                                        + sourcepath
                                        + " : "
                                        + response.getStatusLine()
                                                .getReasonPhrase());
                                println("Exiting to prevent duplicates. Please contact SEAD about the error.");
                                System.exit(1);

                            }
                        } finally {
                            response.close();
                        }
                    } else {
                        //Add the one knownId to the dataset list
                        //Note: the datasets in the list returned by Clowder also have a "name" entry, but we don't use this.
                        datasetList = new JSONArray();
                        JSONObject dataset = new JSONObject();
                        dataset.put("id", knownId);
                        datasetList.put(dataset);
                    }
                    if (datasetList != null) {
                        for (int i = 0; i < datasetList.length(); i++) {
                            String id = datasetList.getJSONObject(i)
                                    .getString("id");
                            String dPath = null;
                            serviceUrl = server + "/api/datasets/" + id
                                    + "/metadata.jsonld";
                            println(serviceUrl);

                            httpget = new HttpGet(appendKeyIfUsed(serviceUrl));
                            if (i % 10 == 0) {
                                //Give some indication of progress
                                System.out.print(".");
                            }
                            response = httpclient.execute(httpget,
                                    getLocalContext());
                            try {
                                if (response.getStatusLine()
                                        .getStatusCode() == 200) {
                                    HttpEntity resEntity = response
                                            .getEntity();
                                    if (resEntity != null) {
                                        JSONArray mdList = new JSONArray(
                                                EntityUtils
                                                        .toString(resEntity));

                                        for (int j = 0; j < mdList.length(); j++) {
                                            if (mdList
                                                    .getJSONObject(j)
                                                    .getJSONObject(
                                                            "content")
                                                    .has("Upload Path")) {
                                                dPath = mdList
                                                        .getJSONObject(
                                                                j)
                                                        .getJSONObject(
                                                                "content")
                                                        .getString(
                                                                "Upload Path")
                                                        .trim();
                                                existingDatasets
                                                        .put(dPath, id);
                                                break;
                                            }
                                        }
                                    }
                                } else {
                                    println("Error response when getting metadata for dataset: "
                                            + id
                                            + " : "
                                            + response.getStatusLine()
                                                    .getReasonPhrase());
                                    println("Exiting to prevent duplicates. Please contact SEAD about the error.");
                                    System.exit(1);

                                }
                            } finally {
                                response.close();
                            }
                            if (dPath != null && sourcepath.equals(dPath)) {
                                //Only scan until we find the right dataset
                                break;
                            }
                        }
                    }
                } catch (IOException e) {
                    println("Error processing check on " + sourcepath
                            + " : " + e.getMessage());
                }
            }
            if (existingDatasets.containsKey(sourcepath)) {
                //If we're looking for a dataset and found it, it's because we've started on a new dataset and need to get new folder/file info
                tagId = existingDatasets.get(sourcepath);
                sead2datasetId = tagId;
                folderMDRetrieved = false;
                fileMDRetrieved = false;
                existingFiles = new HashMap<String, String>();
                existingFolders = new HashMap<String, String>();
            } else {
                if (knownId != null) {
                    //We should have found something - don't continue and create a new dataset
                    println("Dataset with id=" + knownId + "and path: " + sourcepath + " not found.");
                    println("Rerun without the -id flag to scan the entire repository or use -forcenew to force creation of a new dataset.");
                    System.exit(1);
                }
            }

        } else if (item.isDirectory()) {
            /*
				 * /We're looking for a folder Since folders in 2 have no
				 * metadata and can't be moved, we will assume for now that if
				 * the dataset exists and the folder's relative path in the
				 * dataset matches, we've found the folder.
             */
            String sourcepath = relPath + item.getName().trim();
            sourcepath = sourcepath.substring(sourcepath.substring(1)
                    .indexOf("/") + 1);
            if (sead2datasetId != null && !folderMDRetrieved) { // Can't be in a dataset if it
                // wasn't found/created already
                CloseableHttpClient httpclient = getSharedHttpClient();
                try {
                    String serviceUrl = server + "/api/datasets/"
                            + sead2datasetId + "/folders";
                    HttpGet httpget = new HttpGet(appendKeyIfUsed(serviceUrl));
                    CloseableHttpResponse response = httpclient.execute(
                            httpget, getLocalContext());
                    try {
                        if (response.getStatusLine().getStatusCode() == 200) {
                            HttpEntity resEntity = response.getEntity();
                            if (resEntity != null) {
                                JSONArray folders = new JSONArray(
                                        EntityUtils.toString(resEntity));
                                for (int i = 0; i < folders.length(); i++) {
                                    existingFolders.put(folders.getJSONObject(i)
                                            .getString("name"), folders.getJSONObject(i)
                                            .getString("id"));
                                }
                            }
                        } else {
                            println("Error response when checking for existing item at "
                                    + sourcepath
                                    + " : "
                                    + response.getStatusLine()
                                            .getReasonPhrase());
                            println("Exiting to prevent duplicates. Please contact SEAD about the error.");
                            System.exit(1);

                        }
                    } finally {
                        response.close();
                    }
                } catch (IOException e) {
                    println("Error processing check on " + sourcepath
                            + " : " + e.getMessage());
                } finally {
                    folderMDRetrieved = true;
                }
            }
            if (existingFolders.containsKey(sourcepath)) {
                tagId = existingFolders.get(sourcepath);

            }
        } else {
            // A file
            String sourcepath = path + item.getName().trim();

            if (sead2datasetId != null && !fileMDRetrieved) {
                // One-time retrieval of all file id/Upload Path info

                CloseableHttpClient httpclient = getSharedHttpClient();
                try {
                    String serviceUrl = server + "/api/datasets/"
                            + sead2datasetId + "/listAllFiles";

                    HttpGet httpget = new HttpGet(appendKeyIfUsed(serviceUrl));

                    CloseableHttpResponse response = httpclient.execute(
                            httpget, getLocalContext());
                    JSONArray fileList = null;
                    try {
                        if (response.getStatusLine().getStatusCode() == 200) {
                            HttpEntity resEntity = response.getEntity();
                            if (resEntity != null) {
                                fileList = new JSONArray(
                                        EntityUtils.toString(resEntity));
                            }
                        } else {
                            println("Error response when checking for existing item at "
                                    + sourcepath
                                    + " : "
                                    + response.getStatusLine()
                                            .getReasonPhrase());
                            println("Exiting to prevent duplicates. Please contact SEAD about the error.");
                            System.exit(1);

                        }
                    } finally {
                        response.close();
                    }
                    if (fileList != null) {
                        for (int i = 0; i < fileList.length(); i++) {
                            String id = fileList.getJSONObject(i)
                                    .getString("id");
                            serviceUrl = server + "/api/files/" + id
                                    + "/metadata.jsonld";

                            httpget = new HttpGet(appendKeyIfUsed(serviceUrl));

                            response = httpclient.execute(httpget,
                                    getLocalContext());

                            try {
                                if (response.getStatusLine()
                                        .getStatusCode() == 200) {
                                    HttpEntity resEntity = response
                                            .getEntity();
                                    if (resEntity != null) {
                                        JSONArray mdList = new JSONArray(
                                                EntityUtils
                                                        .toString(resEntity));
                                        for (int j = 0; j < mdList.length(); j++) {
                                            if (mdList
                                                    .getJSONObject(j)
                                                    .getJSONObject(
                                                            "content")
                                                    .has("Upload Path")) {

                                                existingFiles
                                                        .put(mdList
                                                                .getJSONObject(
                                                                        j)
                                                                .getJSONObject(
                                                                        "content")
                                                                .getString(
                                                                        "Upload Path")
                                                                .trim(), id);
                                                break;
                                            }
                                        }
                                    }
                                } else {
                                    println("Error response when getting metadata for file: "
                                            + id
                                            + " : "
                                            + response.getStatusLine()
                                                    .getReasonPhrase());
                                    println("Exiting to prevent duplicates. Please contact SEAD about the error.");
                                    System.exit(1);

                                }
                            } finally {
                                response.close();
                            }
                        }

                    }

                } catch (IOException e) {
                    println("Error processing check on " + sourcepath
                            + " : " + e.getMessage());
                } finally {
                    fileMDRetrieved = true;
                }
            }
            if (existingFiles.containsKey(sourcepath)) {
                tagId = existingFiles.get(sourcepath);
            }
        }

        if (verify && (tagId != null) && (!item.isDirectory())) {
            tagId = verifyDataByHash(tagId, path, item);
        }
        return (tagId);
    }

    HashMap<String, String> hashIssues = new HashMap<String, String>();

    protected String verifyDataByHash(String tagId, String path,
            Resource item) {

        String serviceUrl;
        CloseableHttpClient httpclient = getSharedHttpClient();

        try {
            // Work-around - our sead2 servers have issues with incorrect or
            // missing hash values
            // So implementing a direct download and compute option for now.
            // Can be added as a
            // permanent option or replaced with the metadata check later
            serviceUrl = server + "/api/files/"
                    + URLEncoder.encode(tagId, "UTF-8") + "/blob";
            HttpGet httpget = new HttpGet(appendKeyIfUsed(serviceUrl));

            CloseableHttpResponse response = httpclient.execute(httpget,
                    getLocalContext());
            try {
                if (response.getStatusLine().getStatusCode() == 200) {
                    HttpEntity resEntity = response.getEntity();
                    if (resEntity != null) {
                        String realHash = null;
                        InputStream inputStream = resEntity.getContent();
                        realHash = DigestUtils.sha1Hex(inputStream);
                        /*
							 * if (hashtype != null) { if
							 * (hashtype.equals("SHA1 Hash")) { realHash =
							 * DigestUtils.sha1Hex(inputStream);
							 *
							 * } else if (hashtype.equals("SHA512 Hash")) {
							 * realHash = DigestUtils.sha512Hex(inputStream); }
                         */

                        if (realHash != null) {
                            if (!realHash.equals(item.getHash("SHA-1"))) {
                                hashIssues.put(path + item.getName(),
                                        "!!!: A different version of this item exists with ID: "
                                        + tagId);
                                return null;
                            } // else it matches!
                        } else {
                            hashIssues.put(
                                    path + item.getName(),
                                    "Error calculating hash for "
                                    + item.getAbsolutePath()
                                    + " - cannot verify it");
                            return null;
                        }
                    }
                } else {
                    println("Error downloading file to verify "
                            + item.getAbsolutePath() + " : "
                            + response.getStatusLine().getReasonPhrase());

                }
            } finally {
                response.close();
            }
            /*
				 * // sha1: "http://www.w3.org/2001/04/xmldsig-more#sha1"
				 * serviceUrl = server + "/api/files/" +
				 * URLEncoder.encode(tagId, "UTF-8") + "/metadata.jsonld";
				 * HttpGet httpget = new HttpGet(serviceUrl);
				 *
				 * CloseableHttpResponse response = httpclient.execute(httpget,
				 * getLocalContext()); try { if
				 * (response.getStatusLine().getStatusCode() == 200) {
				 * HttpEntity resEntity = response.getEntity(); if (resEntity !=
				 * null) { String json = EntityUtils.toString(resEntity);
				 * JSONArray metadata = new JSONArray(json); String remoteHash =
				 * null; for (int i = 0; i < metadata.length(); i++) {
				 * JSONObject content = metadata.getJSONObject(i)
				 * .getJSONObject("content"); if (content != null) { if
				 * (content.has("sha1")) { remoteHash =
				 * content.getString("sha1"); break; } } } if (remoteHash !=
				 * null) { if (!remoteHash.equals(item.getSHA1Hash())) {
				 * hashIssues.put(path + item.getName(),
				 * "!!!: A different version of this item exists with ID: " +
				 * tagId); return null; } // else it matches! } else {
				 * hashIssues.put(path + item.getName(),
				 * "Remote Hash does not exist for " + item.getAbsolutePath() +
				 * " - cannot verify it"); return null; } } } else {
				 * println("Error response while verifying " +
				 * item.getAbsolutePath() + " : " +
				 * response.getStatusLine().getReasonPhrase());
				 *
				 * } } finally { response.close(); }
             */

        } catch (UnsupportedEncodingException e1) {

            e1.printStackTrace();

        } catch (IOException e) {
            println("Error processing verify on " + item.getAbsolutePath()
                    + " : " + e.getMessage());
        }
        return tagId;
    }

    void addLiteralMetadata(MultipartEntityBuilder meb,
            String predicate, String value) {
        meb.addTextBody(predicate, value);

    }

    void addURIMetadata(MultipartEntityBuilder meb, String predicate,
            String value) {
        meb.addTextBody(predicate, value,
                ContentType.create("text/uri-list", Consts.ISO_8859_1));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void addDatasetMetadata(String newSubject, String type, JSONObject relationships) {

        JSONObject content = new JSONObject();

        JSONObject agent = new JSONObject();
        JSONObject me = get2me();
        agent.put("name", me.getString("fullName"));
        agent.put("@type", "cat:user");
        agent.put("user_id",
                server + "/api/users/" + me.getString("id"));

        for (String predLabel : (Set<String>) relationships
                .keySet()) {
            Object newObject = null;
            if (relationships.get(predLabel) instanceof String) {
                newObject = roCollIdToNewId.get(relationships
                        .getString(predLabel));
                if (newObject != null) {
                    if (newObject.equals(sead2datasetId)) {
                        newObject = server + "/datasets/"
                                + newObject;
                    } else {
                        newObject = server + "/datasets/"
                                + sead2datasetId + "#folderId"
                                + newObject;
                    }
                } else {
                    newObject = roDataIdToNewId
                            .get(relationships
                                    .getString(predLabel));
                    if (newObject != null) {
                        newObject = server + "/files/"
                                + newObject;
                    } else { // Object is not in this Dataset
                        // and
                        // can't be translated - use
                        // original URI
                        newObject = relationships
                                .getString(predLabel);
                    }
                }
                println(newSubject + ": " + predLabel + ": "
                        + newObject.toString());
            } else { // JSONArray
                newObject = new JSONArray();

                JSONArray objects = (JSONArray) relationships
                        .get(predLabel);
                for (int i = 0; i < objects.length(); i++) {
                    String ob = objects.getString(i);
                    String newOb = null;
                    newOb = roCollIdToNewId.get(ob);
                    if (newOb != null) {
                        if (newOb.equals(sead2datasetId)) {
                            newOb = server + "/datasets/"
                                    + newOb;
                        } else {
                            newOb = server + "/datasets/"
                                    + sead2datasetId
                                    + "#folderId" + newOb;
                        }
                    } else {
                        newOb = roDataIdToNewId.get(ob);
                        if (newOb != null) {
                            newOb = server + "/files/" + newOb;
                        } else { // Object is not in this
                            // Dataset and
                            // can't be translated - use
                            // original URI
                            newOb = ob;
                        }
                    }
                    ((JSONArray) newObject).put(newOb);
                }

            }
            println("Writing: " + predLabel + " : "
                    + newObject.toString());
            content.put(predLabel, newObject);

        }
        JSONObject context = new JSONObject();
        context.put("@vocab", CLOWDER_DEFAULT_VOCAB);
        // Create flattened context for 2.0
        for (String key : ((Set<String>) content.keySet())) {
            String pred = rf.getURIForContextEntry(key);
            if (pred != null) {
                context.put(key, pred);
            }
        }
        if (type.equals("datasets")
                || newSubject.equals(sead2datasetId)) {
            CloseableHttpClient httpclient = getSharedHttpClient();

            String uri = server
                    + "/api/"
                    + (type.equals("datasets") ? "files/"
                    : "datasets/") + newSubject
                    + "/metadata.jsonld";
            postMetadata(httpclient, uri, newSubject, content,
                    context, agent);
        } else {
            println("Folder: Would've written: " + newSubject
                    + ": " + content.toString());

        }

    }

    @Override
    protected void postProcessChildren() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void postProcessCollection() {
        // TODO Auto-generated method stub

    }

    @Override
    protected String preprocessCollection(Resource dir, String path, String parentId, String collectionId) throws UploaderException {
        // SEAD2 - create the dataset or folder first before processing
        // children
        if (!listonly) {
            if (collectionId == null) {
                if (parentId == null) {
                    collectionId = create2Dataset(dir, path);
                    sead2datasetId = collectionId;
                } else {
                    collectionId = create2Folder(parentId, sead2datasetId,
                            path, dir);
                    if (collectionId == null) {
                        throw new UploaderException("Failed to create Folder - will not process contents of :" + path);
                    }
                }

            } else {
                // We already have the dataset uploaded so record it's id
                if (parentId == null) {
                    sead2datasetId = collectionId;
                }
            }
        } else {
            if (collectionId != null && parentId == null) {
                sead2datasetId = collectionId;
            }
        }
        if (sead2datasetId != null) {
            println("Dataset ID: " + sead2datasetId);
        }
        return collectionId;
    }

    @Override
    protected String postProcessChild(Resource dir, String path, String parentId, String collectionId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void postProcessDatafile(String newUri, String existingUri, String collectionId, Resource file, Resource dir) throws ClientProtocolException, IOException {

        //IF NOT LISTONLY?
        if (existingUri == null) { // file didn't exist
            // before
            if ((collectionId != null)
                    && (!sead2datasetId
                            .equals(collectionId))) {
                // it's in a folder and not the dataset
                if (newUri != null) { // and it was just
                    // created
                    moveFileToFolder(newUri, collectionId,
                            file);
                }
            }
        } else { // the file existed
            // FixMe - need to check if it is already in the
            // folder or not...
            if (!sead2datasetId.equals(existingUri)) {

                CloseableHttpClient httpclient = getSharedHttpClient();

                HttpGet httpget = new HttpGet(appendKeyIfUsed(server
                        + "/api/datasets/" + sead2datasetId
                        + "/listFiles"));

                CloseableHttpResponse getResponse = httpclient
                        .execute(httpget, getLocalContext());
                try {
                    if (getResponse.getStatusLine()
                            .getStatusCode() == 200) {
                        JSONArray files = new JSONArray(
                                EntityUtils
                                        .toString(getResponse
                                                .getEntity()));
                        for (int i = 0; i < files.length(); i++) {
                            if (files.getJSONObject(i)
                                    .getString("id")
                                    .equals(existingUri)) {
                                // File is in dataset
                                // directly, not in a
                                // folder, so move it if
                                // needed
                                if ((collectionId != null)
                                        && (!sead2datasetId
                                                .equals(collectionId))) { // it's

                                    moveFileToFolder(
                                            existingUri,
                                            collectionId,
                                            file);
                                }
                                break;
                            }
                        }
                    } else {
                        println("Error response when listing files "
                                + dir.getAbsolutePath()
                                + " : "
                                + getResponse
                                        .getStatusLine()
                                        .getReasonPhrase());
                        println("Details: "
                                + EntityUtils
                                        .toString(getResponse
                                                .getEntity()));

                    }
                } finally {
                    EntityUtils.consumeQuietly(getResponse
                            .getEntity());
                    getResponse.close();
                }
            }
        }
    }

    @Override
    protected HttpClientContext reauthenticate(long startTime) {
        if (apiKey != null) {
            return getLocalContext();
        } else {
            return SEADAuthenticator.UPReAuthenticateIfNeeded(server,
                    startTime);
        }
    }

    private String appendKeyIfUsed(String url) {
        if (apiKey != null) {
            if (url.contains("?")) {
                url = url + "&key=" + apiKey;
            } else {
                url = url + "?key=" + apiKey;
            }
        }
        return url;
    }

}
