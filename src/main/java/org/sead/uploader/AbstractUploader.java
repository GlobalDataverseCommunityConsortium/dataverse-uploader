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
package org.sead.uploader;

import org.sead.uploader.util.UploaderException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.protocol.HttpClientContext;
import org.json.JSONObject;
import org.sead.uploader.util.FileResource;
import org.sead.uploader.util.PublishedResource;
import org.sead.uploader.util.Resource;
import org.sead.uploader.util.ResourceFactory;

/**
 * This is an abstract class for uploading file(s) on a disk, a directory, or
 * files in an existing zipped RDA BagIt bag (data file links in the bag's
 * OAI-ORE map must be live for the Uploader to work) to a repository. Extending
 * classes include the specifics for different repository types.
 *
 * In addition to sending files, the Uploader also sends some/all file metadata
 * and performing some mapping to clarify when metadata applies only to the
 * original/published version and when the new live copy 'inherits' the metadata
 * (subject to repository capabilities). This can be adjusted using the black
 * and gray lists of terms and/or providing custom code to map metadata.
 *
 */
public abstract class AbstractUploader {

    protected static AbstractUploader uploader = null;
    protected static final String argSeparator = "=";

    private long max = Long.MAX_VALUE;
    private Long skip = 0l;
    private boolean merge = true;
    protected boolean verify = false;
    protected boolean importRO = false;
    private URL oremapURL = null;
    protected String spaceType = null;

    private long globalFileCount = 0l;
    private long totalBytes = 0L;

    protected boolean listonly = false;

    protected Set<String> excluded = new HashSet<String>();
    protected static List<String> requests = new ArrayList<String>();

    protected static String server = null;

    PrintWriter pw = null;

    HttpClientContext localContext = null;

    protected ResourceFactory rf = null;

    protected HashMap<String, String> roDataIdToNewId = new HashMap<String, String>();
    protected HashMap<String, String> roCollIdToNewId = new HashMap<String, String>();
    protected HashMap<String, String> roFolderProxy = new HashMap<String, String>();

    public void createLogFile(String name) {
        File outputFile = new File(name + "_" + System.currentTimeMillis() + ".log");
        try {
            pw = new PrintWriter(new FileWriter(outputFile));
        } catch (Exception e) {
            println(e.getMessage());
        }
    }

    public static void println(String s) {
        System.out.println(s);
        System.out.flush();
        if (uploader != null) {
            if (uploader.pw != null) {
                uploader.pw.println(s);
                uploader.pw.flush();
            }
        }
        return;
    }
    static DecimalFormat decimalFormat = new DecimalFormat("#.00");

    public static void printStatus(float s) {
        System.out.print("\rProgress: " + decimalFormat.format(s*100) + "%");
        System.out.flush();
        return;
    }

    public void parseArgs(String[] args) {

        for (String arg : args) {
            // println("Arg is : " + arg);
            if (arg.equalsIgnoreCase("-listonly")) {
                listonly = true;
                println("List Only Mode");
            } else if (arg.equals("-forcenew")) {
                merge = false;
                println("Merge mode OFF - will attempt to upload all content");
            } else if (arg.equals("-verify")) {
                verify = true;
                println("Verify Mode: Will verify hash values for file comparisons");
            } else if (arg.equals("-ro")) {
                importRO = true;
                try {
                    oremapURL = new URL(arg.substring(arg.indexOf(argSeparator) + 1));
                } catch (MalformedURLException e) {
                    println("Unable to interpret " + arg.substring(arg.indexOf(argSeparator) + 1) + " as a URL. Exiting.");
                    System.exit(0);
                }
                println("RO Mode: URL for an OREMap is : " + oremapURL.toString());
            } else if (arg.startsWith("-limit")) {
                max = Long.parseLong(arg.substring(arg.indexOf(argSeparator) + 1));
                println("Max ingest file count: " + max);
            } else if (arg.startsWith("-skip")) {
                skip = Long.parseLong(arg.substring(arg.indexOf(argSeparator) + 1));
                println("Skip file count: " + skip);
            } else if (arg.startsWith("-ex")) {
                excluded.add(arg.substring(arg.indexOf(argSeparator) + 1));
                println("Excluding pattern: " + arg.substring(arg.indexOf(argSeparator) + 1));
            } else if (arg.startsWith("-server")) {
                server = arg.substring(arg.indexOf(argSeparator) + 1);
                println("Using server: " + server);
            } else if (!parseCustomArg(arg)) {
                // Upload requests
                println("Request to upload: " + arg);
                requests.add(arg);
            }
        }
    }

    public abstract boolean parseCustomArg(String arg);

    public abstract HttpClientContext authenticate();

    public void processRequests() {
        if (importRO && (skip > 0 || max < Long.MAX_VALUE)) {
            println("Cannot set max or skip limits when importing an existing RO");
            System.exit(0);
        }
        //Avoid max+skip > Long.MAX_VALUE
        max = ((Long.MAX_VALUE - max) - skip) < 0 ? (max - skip) : max;
        localContext = authenticate();
        if (localContext == null) {
            println("Authentication failure - exiting.");
            System.exit(0);
        }
        if (skip > 0) {
            println("WILL SKIP " + skip + " FILES");
        }
        try {
            if (importRO) {
                // Should be a URL
                importRO(oremapURL);
            } else {

                for (String request : requests) {
                    // It's a local path to a file or dir
                    Resource file = new FileResource(request);
                    if (!excluded(file.getName())) {
                        String tagId = null;
                        if (merge) {
                            tagId = itemExists("/", file);
                        }

                        if (file.isDirectory()) {

                            String newUri = uploadCollection(file, "", null, tagId);
                            if (!listonly) { // We're potentially making changes
                                if (newUri == null) { // a collection for path not on
                                    // server or we
                                    // don't care - we have to write
                                    // the
                                    // collection
                                    postProcessCollection();
                                } else {
                                    postProcessChildren(file);
                                }
                            } else {
                                newUri = null; // listonly - report no changes
                            }

                            if (newUri != null) {
                                println("FINALIZING(D): " + file.getPath() + " CREATED as: " + newUri);
                            } else if ((tagId == null) && !listonly) {
                                println("Not uploaded due to error during processing: " + file.getPath());
                            }

                        } else {

                            if ((globalFileCount >= skip) && (globalFileCount < (max + skip))) {
                                String newUri = uploadDatafile(file, null, tagId);
                                if (newUri != null) {
                                    println("              UPLOADED as: " + newUri);
                                    globalFileCount++;
                                    totalBytes += file.length();
                                    println("CURRENT TOTAL: " + globalFileCount + " files :" + totalBytes + " bytes");
                                } else if ((tagId == null) && (!listonly)) {
                                    println("Not uploaded due to error during processing: " + file.getPath());
                                }
                            } else {
                                println("SKIPPING(F):  " + file.getPath());
                                skip--;
                            }
                        }
                    }
                }
            }
            if (pw != null) {
                pw.flush();
                pw.close();
            }
        } catch (Exception e) {
            println(e.getLocalizedMessage());
            e.printStackTrace(pw);
            pw.flush();
            System.exit(1);
        }
    }

    @SuppressWarnings("unchecked")
    private void importRO(URL oremapURL) {
        rf = new ResourceFactory(oremapURL);
        PublishedResource dataset = rf.getParentResource();
        String tagId = null;
        // remove the name and final / char from the absolute path
        String rootPath = dataset.getAbsolutePath().substring(0,
                dataset.getAbsolutePath().length() - dataset.getName().length() - 1);

        if (merge) {

            tagId = itemExists(rootPath + "/", dataset);
        }

        String newUri = uploadCollection(dataset, rootPath, tagId, tagId);

        if (newUri != null) {
            println("              " + dataset.getPath() + " CREATED as: " + newUri);
        } else if ((tagId == null) && !listonly) {
            println("Error processing: " + dataset.getPath());
        }

        JSONObject rels = new JSONObject(PublishedResource.getAllRelationships());
        /*
         * println("Rels to translate"); println(rels.toString(2));
         * println(roCollIdToNewId.toString()); println(roDataIdToNewId.toString());
         */
        if (!listonly) {
            for (String relSubject : (Set<String>) rels.keySet()) {
                JSONObject relationships = rels.getJSONObject(relSubject);
                println(relationships.toString(2));

                String newSubject = null;
                String type = "collections";
                newSubject = roCollIdToNewId.get(findGeneralizationOf(relSubject));

                if (newSubject == null) {
                    newSubject = roDataIdToNewId.get(findGeneralizationOf(relSubject));
                    type = "datasets";
                } else if (roFolderProxy.containsKey(newSubject)) {
                    newSubject = roFolderProxy.get(newSubject);
                    type = "datasets";
                }
                if ((newSubject != null) && (relationships.length() != 0)) {
                    println("Subject: " + newSubject + " for " + relSubject);
                    addDatasetMetadata(newSubject, type, relationships);
                }
            }
        }
    }

    protected String findGeneralizationOf(String id) {
        return id;
    }

    public abstract void addDatasetMetadata(String newSubject, String type, JSONObject relationships);

    protected boolean excluded(String name) {

        for (String s : excluded) {
            if (name.matches(s)) {
                println("Excluding: " + name);
                return true;
            }
        }
        return false;
    }

    protected String uploadCollection(Resource dir, String path, String parentId, String collectionId) {

        new HashSet<String>();
        new HashSet<String>();

        println("\nPROCESSING(D): " + dir.getPath());
        if (collectionId != null) {
            println("              Found as: " + collectionId);
        } else {
            println("              Does not exist on server.");
        }
        if (path != null) {
            path += "/" + dir.getName().trim();
        } else {
            path = "/" + dir.getName().trim();
        }

        boolean created = false;
        String oldCollectionId = collectionId;
        try {
            collectionId = preprocessCollection(dir, path, parentId, oldCollectionId);
            if ((collectionId != null) && (!collectionId.equals(oldCollectionId))) {
                created = true;
            }

            for (Resource file : dir.listResources()) {

                if (excluded(file.getName())) {
                    continue;
                }
                String existingUri = null;
                String newUri = null;

                if (merge) {
                    existingUri = itemExists(path + "/", file);

                }
                /*
                 * Stop processing new items when we hit the limit, but finish writing/adding
                 * children to the current collection.
                 */

                if (globalFileCount < (max + skip)) {

                    /*
                     * If existingUri != null, recursive calls will check children only (and
                     * currently will do nothing for a dataset, but someday may check for changes
                     * and upload a new version.)
                     */
                    if (file.isDirectory()) {
                        newUri = uploadCollection(file, path, collectionId, existingUri);

                        if ((existingUri == null) && (newUri != null)) {
                            println("FINALIZING(D): " + file.getPath() + " CREATED as: " + newUri);
                        }
                    } else {
                        if (globalFileCount >= skip) {
                            // fileStats[1] += file.length();
                            newUri = uploadDatafile(file, path, existingUri);

                            // At this point, For 1.x, dataset is added but not
                            // linked to parent, for 2.0 file is in dataset, but not
                            // in a subfolder
                            postProcessDatafile(newUri, existingUri, collectionId, file, dir);

                            if ((existingUri == null) && (newUri != null)) {
                                globalFileCount++;
                                totalBytes += file.length();
                                println("               UPLOADED as: " + newUri);
                                println("CURRENT TOTAL: " + globalFileCount + " files :" + totalBytes + " bytes");
                            }
                        } else {
                            println("SKIPPING(F):  " + file.getPath());
                            skip = skip - 1;
                            if (skip == 0l) {
                                println("\nSKIP COMPLETE");
                            }
                        }
                    }
                }
                postProcessChild(dir, path, parentId, collectionId);

            }
            if (!listonly) { // We're potentially making changes
                if (collectionId == null) { // a collection for path not on
                    // server or we
                    // don't care - we have to write
                    // the
                    // collection
                    postProcessCollection();
                } else {
                    postProcessChildren(dir);

                }
                if ((collectionId != null) && importRO) {
                    String id = findGeneralizationOf(((PublishedResource) dir).getIdentifier());

                    roCollIdToNewId.put(id, collectionId);
                }
                if (!created) { // item existed before
                    // don't report collection as new
                    collectionId = null;
                }
            } else {
                collectionId = null; // listonly - report no changes
            }
        } catch (IOException io) {
            println("error: " + io.getMessage());// One or more files not
            // uploaded correctly - stop
            // processing...
        } catch (UploaderException e) {
            //Collection preprocessor threw an exception to avoid processing this collection - just send the message and continue
            println(e.getMessage());
        }
        return collectionId;
    }

    protected abstract void postProcessChildren(Resource dir);

    protected abstract void postProcessCollection();

    protected abstract String preprocessCollection(Resource dir, String path, String parentId, String collectionId) throws UploaderException;

    protected abstract String postProcessChild(Resource dir, String path, String parentId, String collectionId);

    protected abstract void postProcessDatafile(String newUri, String existingUri, String collectionId, Resource file,
            Resource dir) throws ClientProtocolException, IOException;

    public String uploadDatafile(Resource file, String path, String dataId) {
        long startTime = System.currentTimeMillis();
        println("\nPROCESSING(F): " + file.getPath());
        if (path != null) {
            path += "/" + file.getName();
        } else {
            path = "/" + file.getName();
        }

        if (dataId != null) {
            println("              Found as: " + dataId);
        } else {
            if (verify) {
                if (hashIssues.containsKey(path)) {
                    println("               " + hashIssues.get(path));
                }
            }
            println("               Does not yet exist on server.");
        }

        boolean created = false;
        if (!listonly) {
            if (dataId == null) { // doesn't exist or we don't care (!merge)
                try {
                    dataId = uploadDatafile(file, path);
                } catch (UploaderException ue) {
                    println(ue.getMessage());
                }
                if (dataId != null) {
                    created = true;
                }
            } else {
                // FixMe - if dataId exists, we could check sha1 and upload a
                // version if file changes
            }
        } else {
            // Increment count if we would have uploaded (dataId==null)
            if (dataId == null) {
                globalFileCount++;
            }
        }
        if ((dataId != null) && importRO) {
            String id = findGeneralizationOf(((PublishedResource) file).getIdentifier());
            roDataIdToNewId.put(id, dataId);
        }
        if (!created) { // Don't report as new (existed or listonly mode)
            dataId = null;
        }

        // If this took a while, try to reauthenticate
        // 30 minutes - a session started by google auth is currently good for 1
        // hour with SEAD, but the JSESSION cookie will timeout if no activity
        // for 30 minutes
        // so we check both for session inactivity > 30 min and google token
        // expiration > 60 min
        // these should always catch any potential timeout and reuathenticate
        // Current values give the Uploader a 100 second window for the next
        // upload to start
        localContext = reauthenticate(startTime);
        if (localContext == null) {
            println("Unable to reauthenticate.");
            System.exit(1);
        }
        ;

        return dataId;
    }

    protected abstract HttpClientContext reauthenticate(long startTime);

    protected abstract String uploadDatafile(Resource file, String path) throws UploaderException;

    public String itemExists(String path, Resource item) {
        //Default is to report the item as not found
        return null;
    }
    ;

    HashMap<String, String> hashIssues = new HashMap<String, String>();

    protected abstract String verifyDataByHash(String tagId, String path, Resource item);

    public static AbstractUploader getUploader() {
        return uploader;
    }

    public static void setUploader(AbstractUploader uploader) {
        AbstractUploader.uploader = uploader;
    }

    public HttpClientContext getLocalContext() {
        return localContext;
    }

    public void setSpaceType(String spaceType) {
        this.spaceType = spaceType;
    }

}
