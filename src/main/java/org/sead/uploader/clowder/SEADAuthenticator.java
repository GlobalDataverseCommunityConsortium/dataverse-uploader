/**
 *
 */
package org.sead.uploader.clowder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.PasswordAuthentication;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author Jim
 *
 */
public class SEADAuthenticator {

    static long token_start_time = -1;
    static int expires_in = -1;

    private static Log log = LogFactory.getLog(SEADAuthenticator.class);

    private static long authTime;
    // Create a local instance of cookie store
    static CookieStore cookieStore = new BasicCookieStore();
    private static HttpClientContext localContext = HttpClientContext.create();

    // Create local HTTP context
    // Bind custom cookie store to the local context
    static {
        localContext.setCookieStore(cookieStore);
    }

    static HttpClientContext authenticate(String server) {

        boolean authenticated = false;
        log.info("Authenticating");

        String accessToken = SEADGoogleLogin.getAccessToken();

        // Now login to server and create a session
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpPost seadAuthenticate = new HttpPost(server
                    + "/api/authenticate");
            List<NameValuePair> nvpList = new ArrayList<NameValuePair>(1);
            nvpList.add(0, new BasicNameValuePair("googleAccessToken",
                    accessToken));

            seadAuthenticate.setEntity(new UrlEncodedFormEntity(nvpList));

            CloseableHttpResponse response = httpclient.execute(
                    seadAuthenticate, localContext);
            try {
                if (response.getStatusLine().getStatusCode() == 200) {
                    HttpEntity resEntity = response.getEntity();
                    if (resEntity != null) {
                        // String seadSessionId =
                        // EntityUtils.toString(resEntity);
                        authenticated = true;
                    }
                } else {
                    // Seems to occur when google device id is not set on server
                    // - with a Not Found response...
                    log.error("Error response from " + server + " : "
                            + response.getStatusLine().getReasonPhrase());
                }
            } finally {
                response.close();
                httpclient.close();
            }
        } catch (IOException e) {
            log.error("Cannot read sead-google.json");
            log.error(e.getMessage());
        }

        // localContext should have the cookie with the SEAD session key, which
        // nominally is all that's needed.
        // FixMe: If there is no activity for more than 15 minutes, the session
        // may expire, in which case,
        // re-authentication using the refresh token to get a new google token
        // to allow SEAD login again may be required
        // also need to watch the 60 minutes google token timeout - project
        // spaces will invalidate the session at 60 minutes even if there is
        // activity
        authTime = System.currentTimeMillis();

        if (authenticated) {
            return localContext;
        }
        return null;
    }

    public static HttpClientContext reAuthenticateIfNeeded(String server,
            long startTime) {
        long curTime = System.currentTimeMillis();
        // If necessary, re-authenticate and return the result
        if (((curTime - startTime) / 1000l > 1700)
                || ((curTime - authTime) / 1000l > 3500)) {
            return UPAuthenticate(server);
        }
        // If it's not time, just return the current value
        return localContext;
    }

    static PasswordAuthentication passwordAuthentication = null;

    static HttpClientContext UPAuthenticate(String server) {

        boolean authenticated = false;
        log.info("Authenticating with username/password");

        File up = new File("./upass.txt");
        if (up.exists()) {
            try {
                BufferedReader bReader = new BufferedReader(new FileReader(up));
                passwordAuthentication = new PasswordAuthentication(bReader.readLine(), bReader.readLine().toCharArray());
                bReader.close();
            } catch (IOException e) {
                System.out.println("Uable to read u/p from file");
                e.printStackTrace();
            }

        }

        if (passwordAuthentication == null) {
            passwordAuthentication = SEAD2UPLogin
                    .getPasswordAuthentication();
        }
        // Now login to server and create a session
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpPost seadAuthenticate = new HttpPost(server
                    + "/authenticate/userpass");
            MultipartEntityBuilder meb = MultipartEntityBuilder.create();
            meb.addTextBody("username", passwordAuthentication.getUserName());
            meb.addBinaryBody("password", toBytes(passwordAuthentication.getPassword()));

            seadAuthenticate.setEntity(meb.build());

            CloseableHttpResponse response = httpclient.execute(
                    seadAuthenticate, localContext);
            HttpEntity resEntity = null;
            try {
                // 303 is a redirect after a successful login, 400 if bad
                // password
                if ((response.getStatusLine().getStatusCode() == 303) || (response.getStatusLine().getStatusCode() == 200)) {
                    resEntity = response.getEntity();
                    if (resEntity != null) {
                        // String seadSessionId =
                        // EntityUtils.toString(resEntity);
                        authenticated = true;
                    }
                } else {
                    // 400 for bad values
                    log.error("Error response from " + server + " : "
                            + response.getStatusLine().getReasonPhrase());
                }
            } finally {
                EntityUtils.consumeQuietly(resEntity);
                response.close();
                httpclient.close();
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }

        // localContext should have the cookie with the SEAD2 session key, which
        // nominally is all that's needed.
        authTime = System.currentTimeMillis();

        if (authenticated) {
            return localContext;
        }

        return null;
    }

    private static byte[] toBytes(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        ByteBuffer byteBuffer = Charset.forName("UTF-8").encode(charBuffer);
        byte[] bytes = Arrays.copyOfRange(byteBuffer.array(),
                byteBuffer.position(), byteBuffer.limit());
        Arrays.fill(charBuffer.array(), '\u0000'); // clear sensitive data
        Arrays.fill(byteBuffer.array(), (byte) 0); // clear sensitive data
        return bytes;
    }

    public static HttpClientContext UPReAuthenticateIfNeeded(String server,
            long startTime) {
        if ((startTime - authTime) > 300000l) { //assume it lasts at least 5*60*1000 msec == 5 min
            return UPAuthenticate(server);
        }
        return localContext;
    }

    private static JSONObject getMe(String server) {
        JSONObject me = null;
        // Now login to server and get user info
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpGet seadGetMe = new HttpGet(server + "/api/me");

            CloseableHttpResponse response = httpclient.execute(seadGetMe,
                    localContext);
            HttpEntity resEntity = null;
            try {
                // 303 is a redirect after a successful login, 400 if bad
                // password
                if (response.getStatusLine().getStatusCode() == 200) {
                    resEntity = response.getEntity();
                    if (resEntity != null) {
                        me = new JSONObject(EntityUtils.toString(resEntity));

                    }
                } else {
                    // 400 for bad values
                    log.error("Error response from " + server + " : "
                            + response.getStatusLine().getReasonPhrase());
                }
            } finally {
                EntityUtils.consumeQuietly(resEntity);
                response.close();
                httpclient.close();
            }
        } catch (IOException e) {

            log.error(e.getMessage());
        }
        return me;
    }
}
