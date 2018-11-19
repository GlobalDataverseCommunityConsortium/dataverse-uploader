/** *****************************************************************************
 * Copyright 2018 Texas Digital Library, jim.myers@computer.org
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

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.sead.uploader.AbstractUploader;
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
    private static boolean oldServer = false;
    private static int maxWaitTime = 60;
    private static boolean recurse = false;

    public static void main(String args[]) throws Exception {

        setUploader(new DVUploader());
        uploader.createLogFile("DVUploaderLog_");
        uploader.setSpaceType("Dataverse")
                ;
        println("\n----------------------------------------------------------------------------------\n");
        println("TTTTT  DDD   L    Texas");
        println("  T    D  D  L    Digital");
        println("  T    DDD   LLL  Library");
        println("");
        println("DVUploader - a command-line application to upload files to any Dataverse Dataset");
        println("Developed for the Dataverse Community");
        println("\n----------------------------------------------------------------------------------\n");
        println("\n***Parsing arguments:***\n");
        uploader.parseArgs(args);
        if (server == null || datasetPID == null || apiKey == null || requests.isEmpty()) {
            println("\n***Required arguments not found.***");
            usage();
        } else {
            println("\n***Starting to Process Upload Requests:***\n");
            uploader.processRequests();
        }
        println("\n***Execution Complete.***");
    }

    private static void usage() {
        println("\nUsage:");
        println("  java -jar DVUploader-1.0.1.jar -server=<serverURL> -key=<apikey> -did=<dataset DOI> <files or directories>");

        println("\n  where:");
        println("      <serverUrl> = the URL of the server to upload to, e.g. https://datverse.tdl.org");
        println("      <apiKey> = your personal apikey, created in the dataverse server at <serverUrl>");
        println("      <did> = the Dataset DOI you are uploading to, e.g. doi:10.5072/A1B2C3");
        println("      <files or directories> = a space separated list of files to upload or directory name(s) where the files to upload are");
        println("\n  Optional Arguments:");
        println("      -listonly    - Scan the Dataset and local files and list what would be uploaded (does not upload with this flag)");
        println("      -limit=<n>   - Specify a maximum number of files to upload per invocation.");
        println("      -verify      - Check both the file name and checksum in comparing with current Dataset entries.");
        println("      -skip=<n>    - a number of files to skip before starting processing (saves time when you know the first n files have been uploaded before)");

        println("      -maxlockwait - the maximum time to wait (in seconds) for a Dataset lock (i.e. while the last file is ingested) to expire (default 60 seconds)");
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
        } else if (arg.equals("-recurse")) {
            recurse = true;
            println("Will recurse into subdirectories");
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

    @Override
    public HttpClientContext authenticate() {
        return new HttpClientContext();
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
            relPath = relPath.substring(relPath.substring(1).indexOf("/") + 1);
            relPath = relPath.substring(relPath.substring(1).indexOf("/") + 1);
        }

        String sourcepath = item.getName();

        // One-time: get metadata for dataset to see if it exists and what files it
        // contains
        if (!datasetMDRetrieved) {
            httpclient = HttpClients.createDefault();

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
                            }   break;
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
                        JSONObject df = datafileList.getJSONObject(i).getJSONObject("dataFile");
                        if (df.has("originalFileFormat") && (!df.getString("contentType").equals(df.getString("originalFileFormat")))) {
                            println("The file named " + df.getString("filename") + " on the server was created by Dataverse's ingest process from an original uploaded file");
                            convertedFiles = true;
                        }
                        existingItems.put(df.getString("filename"), df.getJSONObject("checksum"));
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
                //Directories aren't yet represented in Dataverse
                return null;
            } else {
                // A file within the local directory
                if (existingItems.containsKey(sourcepath)) {
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

    @Override
    public void addDatasetMetadata(String newSubject, String type, JSONObject relationships) {
        // TBD
        // println("DVUploader does not yet add metadata to a Dataset");
    }

    @Override
    protected void postProcessChildren() {
        // TBD
        // println("DVUploader does not need to post-process after files are uploaded");
    }

    @Override
    protected void postProcessCollection() {
        // TBD
        // println("DVUploader does not yet support creation of datasets or uploading
        // sub-directories and their contents");

    }

    @Override
    protected String preprocessCollection(Resource dir, String path, String parentId, String collectionId) throws UploaderException {
        if (!path.equals("/" + dir.getName().trim()) && !recurse) {
            throw new UploaderException("              DVUploader is not configured to recurse into sub-directories.");
        }
        return null;
    }

    @Override
    protected String postProcessChild(Resource dir, String path, String parentId, String collectionId) {
        // TBD
        // println("DVUploader does not need to post-process newly created items");
        return null;
    }

    @Override
    protected void postProcessDatafile(String newUri, String existingUri, String collectionId, Resource file, Resource dir) throws ClientProtocolException, IOException {
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
        if (httpclient == null) {
            httpclient = HttpClients.createDefault();
        }
        String dataId = null;
        int retry = 10;

        while (retry > 0) {

            try {
                // Now post data
                String urlString = server + "/api/datasets/:persistentId/add";
                urlString = urlString + "?persistentId=" + datasetPID + "&key=" + apiKey;
                HttpPost httppost = new HttpPost(urlString);

                ContentBody bin = file.getContentBody();

                MultipartEntityBuilder meb = MultipartEntityBuilder.create();
                meb.addPart("file", bin);

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
                        retry = 0;
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
                            retry--;
                        } else {
                            println("Error response when processing " + file.getAbsolutePath() + " : "
                                    + response.getStatusLine().getReasonPhrase());
                            // A real error: e.g. This file already exists in the dataset.
                            if (res != null) {
                                println(res);
                            }
                            // Skip
                            retry = 0;
                        }
                    } else {
                        // An error and unlikely that we can recover, so report and move on.
                        println("Error response when processing " + file.getAbsolutePath() + " : "
                                + response.getStatusLine().getReasonPhrase());
                        if (res != null) {
                            println(res);
                        }
                        retry = 0;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    retry = 0;
                } finally {
                    EntityUtils.consumeQuietly(response.getEntity());
                }

            } catch (IOException e) {
                println("Error processing " + file.getAbsolutePath() + " : " + e.getMessage());
                retry = 0;
            }
        }
        return dataId;
    }

    private boolean isLocked() {
        if (httpclient == null) {
            httpclient = HttpClients.createDefault();
        }
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
}
