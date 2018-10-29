/**
 *
 */
package org.sead.uploader.clowder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author Jim
 *
 */
public class SEADGoogleLogin {

    static private GoogleProps gProps = null;
    static private String refresh_token = null;
    static private String access_token = null;
    static private String verification_url = null;
    static private String user_code = null;
    static private String device_code = null;
    static long token_start_time = -1;
    static int expires_in = -1;

    private static Log log = LogFactory.getLog(SEADGoogleLogin.class);

    /**
     * @param args
     */
    public static void main(String[] args) {

        String accessToken = getAccessToken();

        if (accessToken != null) {
            System.out.println("New Access Token is: " + accessToken);
            System.out.println("Expires in " + expires_in + " seconds");
        }
    }

    public static String getAccessToken() {
        try {
            refresh_token = new String(Files.readAllBytes(Paths
                    .get("refresh.txt")));
        } catch (IOException e1) {
        }
        if (refresh_token == null) {

            getAuthCode();

            // Ask user to login via browser
            System.out
                    .println("Did not find stored refresh token. Initiating first-time device authorization request.\n");
            System.out.println("1) Go to : " + verification_url
                    + " in your browser\n");
            System.out.println("2) Type : " + user_code + " in your browser\n");
            System.out.println("3) Hit <Return> to continue.\n");
            try {
                System.in.read();
            } catch (IOException e) {
                log.debug("Error getting user response: " + e.getMessage());
            }

            System.out.println("Proceeding");
            getTokensFromCode();

            if (refresh_token != null) {
                PrintWriter writer = null;
                try {
                    writer = new PrintWriter("refresh.txt", "UTF-8");
                    writer.print(refresh_token);

                } catch (FileNotFoundException e) {
                    log.error("Could not write refresh.txt: " + e.getMessage());
                } catch (UnsupportedEncodingException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } finally {
                    if (writer != null) {
                        writer.close();
                    }
                }

            }
        } else {
            getTokenFromRefreshToken();
        }
        return access_token;

    }

    private static void initGProps() {
        ObjectMapper mapper = new ObjectMapper();
        // Read Google Oauth2 info
        try {
            gProps = mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE,
                    true).readValue(new File("sead-google.json"),
                            GoogleProps.class);
        } catch (Exception e) {
            log.error("Error reading sead-google.json file: " + e.getMessage());
        }

    }

    static void getAuthCode() {
        access_token = null;
        expires_in = -1;
        token_start_time = -1;
        refresh_token = null;
        new File("refresh.txt").delete();

        if (gProps == null) {
            initGProps();
        }

        // Contact google for a user code
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {

            String codeUri = gProps.auth_uri.substring(0,
                    gProps.auth_uri.length() - 4)
                    + "device/code";

            HttpPost codeRequest = new HttpPost(codeUri);

            MultipartEntityBuilder meb = MultipartEntityBuilder.create();
            meb.addTextBody("client_id", gProps.client_id);
            meb.addTextBody("scope", "email profile");
            HttpEntity reqEntity = meb.build();

            codeRequest.setEntity(reqEntity);
            CloseableHttpResponse response = httpclient.execute(codeRequest);
            try {

                if (response.getStatusLine().getStatusCode() == 200) {
                    HttpEntity resEntity = response.getEntity();
                    if (resEntity != null) {
                        String responseJSON = EntityUtils.toString(resEntity);
                        ObjectNode root = (ObjectNode) new ObjectMapper()
                                .readTree(responseJSON);
                        device_code = root.get("device_code").asText();
                        user_code = root.get("user_code").asText();
                        verification_url = root.get("verification_url")
                                .asText();
                        expires_in = root.get("expires_in").asInt();
                    }
                } else {
                    log.error("Error response from Google: "
                            + response.getStatusLine().getReasonPhrase());
                }
            } finally {
                response.close();
                httpclient.close();
            }
        } catch (IOException e) {
            log.error("Error reading sead-google.json or making http requests for code.");
            log.error(e.getMessage());
        }
    }

    static void getTokensFromCode() {
        access_token = null;
        expires_in = -1;
        token_start_time = -1;
        refresh_token = null;
        new File("refresh.txt").delete();

        if (gProps == null) {
            initGProps();
        }
        // Query for token now that user has gone through browser part
        // of
        // flow
        HttpPost tokenRequest = new HttpPost(gProps.token_uri);

        MultipartEntityBuilder tokenRequestParams = MultipartEntityBuilder
                .create();
        tokenRequestParams.addTextBody("client_id", gProps.client_id);
        tokenRequestParams.addTextBody("client_secret", gProps.client_secret);
        tokenRequestParams.addTextBody("code", device_code);
        tokenRequestParams.addTextBody("grant_type",
                "http://oauth.net/grant_type/device/1.0");

        HttpEntity reqEntity = tokenRequestParams.build();

        tokenRequest.setEntity(reqEntity);
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        try {
            response = httpclient.execute(tokenRequest);

            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    String responseJSON = EntityUtils.toString(resEntity);
                    ObjectNode root = (ObjectNode) new ObjectMapper()
                            .readTree(responseJSON);
                    access_token = root.get("access_token").asText();
                    refresh_token = root.get("refresh_token").asText();
                    token_start_time = System.currentTimeMillis() / 1000;
                    expires_in = root.get("expires_in").asInt();
                }
            } else {
                log.error("Error response from Google: "
                        + response.getStatusLine().getReasonPhrase());
            }
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            try {
                httpclient.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

    }

    static void getTokenFromRefreshToken() {
        access_token = null;
        expires_in = -1;
        token_start_time = -1;

        if (gProps == null) {
            initGProps();
        }

        /* Try refresh token */
        // Query for token now that user has gone through browser part
        // of
        // flow
        // The method used in getTokensFromCode should work here as well - I
        // think URL encoded Form is the recommended way...
        HttpPost post = new HttpPost(gProps.token_uri);
        post.addHeader("accept", "application/json");
        post.addHeader("Content-Type", "application/x-www-form-urlencoded");

        List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>(4);
        nameValuePairs
                .add(new BasicNameValuePair("client_id", gProps.client_id));
        nameValuePairs.add(new BasicNameValuePair("client_secret",
                gProps.client_secret));
        nameValuePairs.add(new BasicNameValuePair("refresh_token",
                refresh_token));
        nameValuePairs
                .add(new BasicNameValuePair("grant_type", "refresh_token"));

        try {
            post.setEntity(new UrlEncodedFormEntity(nameValuePairs));
        } catch (UnsupportedEncodingException e1) {
            e1.printStackTrace();
        }

        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        try {
            try {
                response = httpclient.execute(post);
                if (response.getStatusLine().getStatusCode() == 200) {
                    HttpEntity resEntity = response.getEntity();
                    if (resEntity != null) {
                        String responseJSON = EntityUtils.toString(resEntity);
                        ObjectNode root = (ObjectNode) new ObjectMapper()
                                .readTree(responseJSON);
                        access_token = root.get("access_token").asText();
                        // refresh_token =
                        // root.get("refresh_token").asText();
                        token_start_time = System.currentTimeMillis() / 1000;
                        expires_in = root.get("expires_in").asInt();
                    }
                } else {
                    log.error("Error response from Google: "
                            + response.getStatusLine().getReasonPhrase());
                    HttpEntity resEntity = response.getEntity();
                }
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                log.error("Error obtaining access token: " + e.getMessage());
            } finally {
                if (response != null) {
                    response.close();
                }
                httpclient.close();
            }
        } catch (IOException io) {
            log.error("Error closing connections: " + io.getMessage());
        }
    }
}
