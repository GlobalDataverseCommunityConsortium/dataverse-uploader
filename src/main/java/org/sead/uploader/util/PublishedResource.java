/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sead.uploader.util;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import javax.json.Json;
import javax.json.JsonObjectBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author qqmye
 */
public class PublishedResource extends Resource {
    
    protected static ResourceFactory myFactory;

    public static void setResourceFactory(ResourceFactory rf) {
        myFactory = rf;
    }
    protected JSONObject resource;
    protected String path;
    protected String origTitle = null;
    HashMap<String, Object> origMD = new HashMap<String, Object>();
    protected String absPath;

    public PublishedResource() {
    }
    
    public PublishedResource(JSONObject jo) {
        resource = jo;
        System.out.println("JO: " + jo.toString(2));
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
                    if ("http://cet.ncsa.uiuc.edu/2007/Collection".equals(type) || "http://cet.ncsa.uiuc.edu/2016/Folder".equals(type)) {
                        return true;
                    }
                }
            } else if (o instanceof String) {
                String type = ((String) o).trim();
                // 1.5 and 2.0 types
                if ("http://cet.ncsa.uiuc.edu/2007/Collection".equals(type) || "http://cet.ncsa.uiuc.edu/2016/Folder".equals(type)) {
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
            return new InputStreamBody(entity.getContent(), ContentType.create(resource.getString("Mimetype")), getName());
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

    @Override
    public String getHash(String algorithm) {
        String hash = null;
        if (algorithm.equals("SHA-1")) {
            hash = resource.getString("SHA1 Hash");
        }
        return hash;
    }

    @SuppressWarnings(value = "unchecked")
    @Override
    public JSONObject getMetadata() {
        ArrayList<String> keysToKeep = new ArrayList<String>();
        keysToKeep.addAll(resource.keySet());
        keysToKeep.remove("Has Part");
        keysToKeep.remove("aggregates");
        JSONObject md = new JSONObject(resource, keysToKeep.toArray(new String[keysToKeep.size()]));
        md.put("@context", myFactory.orgJsonbaseContext);
        return md;
    }

    public String getIdentifier() {
        return resource.getString("@id");
    }

    @Override
    public String getMimeType() {
        if (resource.has("MimeType")) {
            return resource.getString("MimeType");
        }
        return null;
    }

    public void setAbsolutePath(String abspath) {
        absPath = abspath;
    }

    public void setPath(String path) {
        this.path = path;
    }
    
}
