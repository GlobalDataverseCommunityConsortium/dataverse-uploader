/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sead.uploader.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.json.JSONException;
import org.json.JSONObject;
import static org.sead.uploader.util.PublishedResource.myFactory;

/**
 *
 * @author qqmye
 */
public class BagResource extends PublishedResource {
    
    public BagResource() {
        super();
    }
    
    public BagResource(JSONObject jo) {
        super(jo);
    }

    @Override
    public ContentBody getContentBody() {
        return new InputStreamBody(getInputStream(), ContentType.create(resource.getString("Mimetype")), getName());
    }

    @Override
    public InputStream getInputStream() {
        System.out.println("Getting ID for " + this.getIdentifier());
        String path = ((BagResourceFactory) myFactory).getBasePath() + "/" + ((BagResourceFactory) myFactory).pidMap.get(this.getIdentifier());
        System.out.println("Getting path " + path);
        ZipEntry ze = ((BagResourceFactory) myFactory).getZipFile().getEntry(path);
        InputStream in;
        try {
            in = ((BagResourceFactory) myFactory).getZipFile().getInputStream(ze);
        } catch (IOException ex) {
            Logger.getLogger(BagResource.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
        return in;
    }

}
