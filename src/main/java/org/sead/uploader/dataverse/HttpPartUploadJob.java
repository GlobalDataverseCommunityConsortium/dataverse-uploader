/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sead.uploader.dataverse;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import static org.sead.uploader.AbstractUploader.println;
import org.sead.uploader.util.Resource;

/**
 *
 * @author Jim
 */
public class HttpPartUploadJob implements Runnable {

    int partNo;
    long size;
    String signedUrl;
    Resource file;
    Map eTags;

    static private long partSize = -1;
    static private CloseableHttpClient httpClient = null;
    static private HttpClientContext localContext = null;

    public static void setHttpClient(CloseableHttpClient client) {
        httpClient = client;
    }

    public static void setHttpClientContext(HttpClientContext context) {
        localContext = context;
    }
    
    public static void setPartSize(long ps) {
        partSize=ps;
    }

    public HttpPartUploadJob(int partNo, String url, Resource file, long size, Map eTags) throws IllegalStateException {
        if ((size == -1) || (httpClient == null) || (localContext == null)) {
            throw new IllegalStateException("partSize not set");
        }
        this.partNo = partNo;
        this.signedUrl = url;
        this.file = file;
        this.size=size;
        this.eTags = eTags;
    }

    /*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
     */
    public void run() {
        int retries = 3;
        //println("Starting upload of part: " + partNo);
        while (retries > 0) {
            try (InputStream is = file.getInputStream((partNo - 1) * partSize, size)) {

                HttpPut httpput = new HttpPut(signedUrl);
                httpput.setEntity(new InputStreamEntity(is, size));
                CloseableHttpResponse putResponse = httpClient.execute(httpput);
                int putStatus = putResponse.getStatusLine().getStatusCode();
                String putRes = null;
                HttpEntity putEntity = putResponse.getEntity();
                if (putEntity != null) {
                    putRes = EntityUtils.toString(putEntity);
                }
                if (putStatus == 200) {
                    //Part successfully stored - parse the eTag from the response and it it to the Map
                    String eTag = putResponse.getFirstHeader("ETag").getValue();
                    eTag= eTag.replace("\"","");
                    eTags.put(Integer.toString(partNo), eTag);
                    retries = 0;
                    //println("Completed upload of part: " + partNo);
                } else {
                    if (putStatus >= 500) {
                        println("Upload of part: " + partNo + " failed with status: " + putStatus + " (skipping)");
                        println("Error response: " + putResponse.getStatusLine() + " : " + putRes);
                        retries--;
                    } else {
                        println("Upload of part: " + partNo + " failed with status: " + putStatus + " (retrying)");
                        println("Error response: " + putResponse.getStatusLine() + " : " + putRes);

                        retries--;
                    }
                }

            } catch (IOException e) {
                e.printStackTrace(System.out);
                println("Error uploading part: " + partNo + " : " + e.getMessage());
                retries = 0;
            }
        }
    }
}
