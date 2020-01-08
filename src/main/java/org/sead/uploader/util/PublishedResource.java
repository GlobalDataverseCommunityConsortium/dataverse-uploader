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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class PublishedResource implements Resource {

    protected JSONObject resource;
    private String path;
    private String absPath;
    private String origTitle = null;
    private static ResourceFactory myFactory;

    static final HashMap<String, String> blackList = new HashMap<String, String>() {
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        {
            put("Purpose", "http://sead-data.net/vocab/publishing#Purpose");
            put("Rights Holder", "http://purl.org/dc/terms/rightsHolder");
            put("Repository", "http://sead-data.net/terms/requestedrepository");
            put("Max Dataset Size", "http://sead-data.net/terms/maxdatasetsize");
            put("Total Size", "tag:tupeloproject.org,2006:/2.0/files/length");
            put("Aggregation Statistics", "http://sead-data.net/terms/publicationstatistics");

            put("Number of Datasets", "http://sead-data.net/terms/datasetcount");
            put("Preferences", "http://sead-data.net/terms/publicationpreferences");
            put("Publication Callback", "http://sead-data.net/terms/publicationcallback");
            put("Max Collection Depth", "http://sead-data.net/terms/maxcollectiondepth");
            put("Number of Collections", "http://sead-data.net/terms/collectioncount");
            put("Affiliations", "http://sead-data.net/terms/affiliations");

            put("Publishing Project", "http://sead-data.net/terms/publishingProject");
            put("Publishing Project Name", "http://sead-data.net/terms/publishingProjectName");
            // "https://w3id.org/ore/context",
            put("Has Part", "http://purl.org/dc/terms/hasPart");
            put("Is Version Of", "http://purl.org/dc/terms/isVersionOf");
            put("@type", "@type");
            put("@id", "@id");
            put("similarTo", "http://www.openarchives.org/ore/terms/similarTo");
            put("SHA1 Hash", "http://sead-data.net/terms/hasSHA1Digest");
            put("Size", "tag:tupeloproject.org,2006:/2.0/files/length");
            put("Mimetype", "http://purl.org/dc/elements/1.1/format"); // used
            // directly
            // to
            // be
            // the
            // mimetype
            // for
            // the
            // uploaded
            // file

        }
    };

    static final HashMap<String, String> grayList = new HashMap<String, String>() {
        private static final long serialVersionUID = 1L;

        {

            // put("Creator","http://purl.org/dc/terms/creator");
            // put("Contact","http://sead-data.net/terms/contact");
            put("Creation Date", "http://purl.org/dc/terms/created");
            put("Data Mimetypes", "http://purl.org/dc/elements/1.1/format");
            put("Uploaded By", "http://purl.org/dc/elements/1.1/creator");
            put("Date", "http://purl.org/dc/elements/1.1/date");

            put("Label", "http://www.w3.org/2000/01/rdf-schema#label");
            put("Keyword", "http://www.holygoat.co.uk/owl/redwood/0.1/tags/taggedWithTag");
            put("Publication Date", "http://purl.org/dc/terms/issued");
            put("GeoPoint", "tag:tupeloproject.org,2006:/2.0/gis/hasGeoPoint");
            put("Comment", "http://cet.ncsa.uiuc.edu/2007/annotation/hasAnnotation");

            /*
			 * put("GeoPoint: { put("@id: "tag:tupeloproject.org,2006:/2.0/gis/hasGeoPoint",
			 * long");"http://www.w3.org/2003/01/geo/wgs84_pos#long",
			 * lat");"http://www.w3.org/2003/01/geo/wgs84_pos#lat" }, put("Comment: {
			 * put("@id: "http://cet.ncsa.uiuc.edu/2007/annotation /hasAnnotation",
			 * put("comment_body: "http://purl.org/dc/elements/1.1 /description");
			 * put("comment_author: "http://purl.org/dc/elements/1.1/creator",
			 * comment_date");"http://purl.org/dc/elements/1.1/date" },
             */
            put("Instance Of", "http://purl.org/vocab/frbr/core#embodimentOf");
            put("External Identifier", "http://purl.org/dc/terms/identifier");
            put("License", "http://purl.org/dc/terms/license");
            put("Rights Holder", "http://purl.org/dc/terms/rightsHolder");
            put("Rights", "http://purl.org/dc/terms/rights");
            put("Created", "http://purl.org/dc/elements/1.1/created");
            put("Size", "tag:tupeloproject.org,2006:/2.0/files/length");
            put("Label", "http://www.w3.org/2000/01/rdf-schema#label");
            put("Identifier", "http://purl.org/dc/elements/1.1/identifier");
        }
    };

    public static void setResourceFactory(ResourceFactory rf) {
        myFactory = rf;
    }

    public PublishedResource(JSONObject jo) {
        resource = jo;
    }

    @Override
    public String getName() {
        String name = resource.getString("Label");
        if (resource.has("Title")) {
            origTitle = resource.getString("Title");
        }
        if (isDirectory()) {
            /*
			 * Since Clowder doesn't keep the original name of a folder if it is changed,
			 * and we use the name in paths, we can't write the name and change the visible
			 * label like we do with Files (done because labels may not be valid filenames
			 * if they have unicode chars, etc.). So - a work-around is to always use the
			 * title for collections and ignore the Label This means the Label is lost for
			 * 1.5 collections - acceptable since, while it is captured in a publication, it
			 * is not visible to the user via the GUI.
             */
            if (origTitle == null || (origTitle.isEmpty())) {
                name = "<no name>";
            } else {
                name = origTitle;
            }
        }
        // Label should always exist and be valid....
        if (name == null || name.isEmpty()) {
            System.err.println("Warning: Bad Label found for resource with title: " + origTitle);
            // Handle rare/test cases where all are empty strings
            if (origTitle == null || (origTitle.isEmpty())) {
                name = "<no name>";
            } else {
                name = origTitle;
            }
        }
        return name;
    }

    @Override
    public boolean isDirectory() {
        if (getChildren().length() != 0) {
            return true;
        }
        Object o = resource.get("@type");
        if (o != null) {
            if (o instanceof JSONArray) {
                for (int i = 0; i < ((JSONArray) o).length(); i++) {
                    String type = ((JSONArray) o).getString(i).trim();
                    // 1.5 and 2.0 types
                    if ("http://cet.ncsa.uiuc.edu/2007/Collection".equals(type)
                            || "http://cet.ncsa.uiuc.edu/2016/Folder".equals(type)) {
                        return true;
                    }

                }
            } else if (o instanceof String) {
                String type = ((String) o).trim();
                // 1.5 and 2.0 types
                if ("http://cet.ncsa.uiuc.edu/2007/Collection".equals(type)
                        || "http://cet.ncsa.uiuc.edu/2016/Folder".equals(type)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public Iterator<Resource> iterator() {
        return listResources().iterator();
    }

    @Override
    public Iterable<Resource> listResources() {
        ArrayList<Resource> resources = new ArrayList<Resource>();
        JSONArray children = getChildren();
        for (int i = 0; i < children.length(); i++) {
            resources.add(myFactory.getPublishedResource(children.getString(i), getPath()));
        }
        return resources;
    }

    // Get's all "Has Part" children, standardized to send an array with 0,1, or
    // more elements
    JSONArray getChildren() {
        Object o = null;
        try {
            o = resource.get("Has Part");
        } catch (JSONException e) {
            // Doesn't exist - that's OK
        }
        if (o == null) {
            return new JSONArray();
        } else {
            if (o instanceof JSONArray) {
                return (JSONArray) o;
            } else if (o instanceof String) {
                return new JSONArray("[	" + (String) o + " ]");
            }
            return new JSONArray();
        }
    }

    @Override
    public long length() {
        long size = 0;
        if (!isDirectory()) {
            size = Long.parseLong(resource.optString("Size")); // sead2 sends a
            // number, 1.5 a
            // string
        }
        return size;
    }

    @Override
    public String getAbsolutePath() {
        return absPath;
    }

    @Override
    public ContentBody getContentBody() {
        // While space and / etc. are already encoded, the quote char is not and
        // it is not a valid char
        // Fix Me - identify additional chars that need to be encoded...
        String uri = resource.getString("similarTo").replace("\"", "%22").replace(";", "%3b");

        try {
            HttpEntity entity = myFactory.getURI(new URI(uri));
            return new InputStreamBody(entity.getContent(), ContentType.create(resource.getString("Mimetype")),
                    getName());
        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public InputStream getInputStream() {
        // While space and / etc. are already encoded, the quote char is not and
        // it is not a valid char
        // Fix Me - identify additional chars that need to be encoded...
        String uri = resource.getString("similarTo").replace("\"", "%22").replace(";", "%3b");

        try {
            HttpEntity entity = myFactory.getURI(new URI(uri));
            return entity.getContent();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setAbsolutePath(String abspath) {
        absPath = abspath;
    }

    @Override
    public String getHash(String algorithm) {
        String hash = null;
        if (algorithm.equals("SHA-1")) {
            hash = resource.getString("SHA1 Hash");
        }
        return hash;
    }

    private static HashMap<String, Object> allRelationships = new HashMap<String, Object>();

    @SuppressWarnings("unchecked")
    @Override
    public JSONObject getMetadata() {
        ArrayList<String> keysToKeep = new ArrayList<String>();
        keysToKeep.addAll(resource.keySet());
        HashMap<String, Object> relationships = new HashMap<String, Object>();

        HashMap<String, Object> changed = new HashMap<String, Object>();
        for (String key : (Set<String>) resource.keySet()) {
            if (blackList.containsKey(key)) {
                // SEADUploader.println("Found: " + key + " : dropped");
                keysToKeep.remove(key);
            } else if (grayList.containsKey(key)) {
                // SEADUploader.println("Found: " + key + " : converted");
                if (!willConvert(key, resource.get(key))) {
                    keysToKeep.remove(key);
                }
            } else {
                // else keep it, including the @context
                // SEADUploader.println("Found: " + key + " : keeping");
            }

        }
        // SEADUploader.println(resource.toString());
        JSONObject md = new JSONObject(resource, keysToKeep.toArray(new String[keysToKeep.size()]));
        // Note @context may have unnecessary elements now - should not be
        // harmful but could perhaps be removed
        // SEADUploader.println(md.toString());

        for (String key : (Set<String>) md.keySet()) {
            // SEADUploader.println("Checking: " + key + " : "
            // + md.get(key).toString());
            if (md.get(key) instanceof String) {
                String val = resource.getString(key);
                // SEADUploader.println(key + " : " + val);
                if (val.startsWith("tag:") || val.startsWith("urn:")) {

                    relationships.put(key, val);
                } else {
                    Object updated = convert(key, val);
                    if (updated != null) {
                        changed.put(key, updated);
                    }
                }
            } else if (md.get(key) instanceof JSONArray) {
                JSONArray vals = md.getJSONArray(key);
                JSONArray newvals = new JSONArray();
                JSONArray newrels = new JSONArray();

                for (int i = 0; i < vals.length(); i++) {
                    // SEADUploader.println("Checking: " + i + " : "
                    // + vals.get(i).toString());
                    if (vals.get(i) instanceof String) {
                        // relationships always have a string value by definition
                        if (vals.getString(i).startsWith("tag:") || vals.getString(i).startsWith("urn:")) {
                            newrels.put(vals.getString(i));
                        } else {
                            Object updated = convert(key, vals.getString(i));
                            if (updated != null) {
                                newvals.put(updated);
                            }
                        }
                    } else {
                        Object updated = convert(key, vals.get(i));
                        if (updated != null) {
                            newvals.put(updated);
                        }
                    }
                }
                if (newvals.length() != 0) {
                    changed.put(key, newvals);
                }
                if (newrels.length() != 0) {
                    relationships.put(key, newrels);
                }
            } else {
                changed.put(key, md.get(key));
            }
        }
        md = new JSONObject(changed);
        md.put("Metadata on Original", origMD);

        allRelationships.put(resource.getString("Identifier"), relationships);
        return md;

    }

    HashMap<String, Object> origMD = new HashMap<String, Object>();

    private boolean willConvert(String key, Object object) {

        if (!ResourceFactory.grayConversions.containsKey(key)) {
            if (key.equals("Label")) {
                if ((origTitle != null) && (!((String) object).equals(origTitle))) {
                    // It's unique - move it to orig metadata
                    origMD.put(key, object);
                } else {
                    // It's the same as the name/title - don't move it to the
                    // original metadata
                }
            } else {
                origMD.put(key, object);
            }
            // Regardless, don't keep the original item
            return false;
        }
        return true;
    }

    private Object convert(String key, Object object) {
        switch (key) {
            case "Creator":
                if (object instanceof JSONObject) {
                    object = ((JSONObject) object).getString("@id");
                }
                break;
            case "Contact":
                if (object instanceof JSONObject) {
                    object = ((JSONObject) object).getString("@id");
                }
                break;
            case "External Identifier":
                if (object instanceof String) {
                    if (((String) object).startsWith("http://doi.org/10.5072/")) {
                        object = null;
                    }
                }
        }
        return object;
    }

    public static HashMap<String, Object> getAllRelationships() {
        return allRelationships;
    }

    public String getIdentifier() {
        return resource.getString("Identifier");

    }

    /*
	 * For datasets, we send the abstract as a description - before processing
	 * metadata in general, so retrieve the value and then remove it so that
	 * duplicate metadata is not sent.
     */
    public String getAndRemoveAbstract(boolean d2a) {
        String theAbstract = null;
        if (d2a) {
            if (resource.has("Has Description")) {
                Object descObject = resource.get("Has Description");
                if (descObject instanceof String) {
                    theAbstract = descObject.toString();
                } else if (descObject instanceof JSONArray) {
                    theAbstract = ((JSONArray) descObject).toString();
                }
                resource.remove("Has Description");
            }
        }
        if (resource.has("Abstract")) {
            if (theAbstract == null) {
                theAbstract = "";
            } else { // Combining with a description - add a space between
                theAbstract = theAbstract + " ";
            }
            if (resource.get("Abstract") instanceof JSONArray) {
                // Convert multiple abstracts into 1 so it fits
                // Clowder's single description field
                // Could concatenate, but JSON will help if anyone wants
                // to separate abstracts after migration
                theAbstract = theAbstract + ((JSONArray) resource.getJSONArray("Abstract")).toString(2);
            } else {
                theAbstract = theAbstract + resource.getString("Abstract").toString();
            }
            resource.remove("Abstract");
        }
        return theAbstract;
    }

    /*
	 * return the "Title" (which may be different than getName which comes from the
	 * "Label"
     */
    public String getAndRemoveTitle() {

        if (resource.has("Title")) {
            origTitle = resource.getString("Title");
            resource.remove("Title");
        }
        return origTitle;
    }

    /*
	 * For datasets, we send the creators via the dataset api - before processing
	 * metadata, so retrieve the value and then remove it so that duplicate metadata
	 * is not sent.
     */
    public void getAndRemoveCreator(List<String> creators) {

        if (resource.has("Creator")) {
            Object creatorField = resource.get("Creator");
            if (creatorField instanceof JSONArray) {
                for (int i = 0; i < ((JSONArray) creatorField).length(); i++) {
                    Object creator = ((JSONArray) creatorField).get(i);
                    if (creator instanceof JSONObject) {
                        creators.add(((String) convert("Creator", creator)));
                    } else {
                        creators.add((String) creator);
                    }
                }
            } else if (creatorField instanceof JSONObject) {
                creators.add(((String) convert("Creator", creatorField)));
            } else {
                creators.add(((String) creatorField));
            }
            resource.remove("Creator");
        }
    }

    @Override
    public String getMimeType() {
        if (resource.has("MimeType")) {
            return resource.getString("MimeType");
        }
        return null;
    }
}
