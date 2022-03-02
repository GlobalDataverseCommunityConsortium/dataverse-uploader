/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sead.uploader.dataverse;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import static org.sead.uploader.AbstractUploader.println;
import org.sead.uploader.util.Resource;

/**
 *
 * @author Jim
 */
public class MD5Job implements Runnable {

    Resource file;
    Map infoMap;

    public MD5Job(Resource file, Map infoMap) throws IllegalStateException {
        this.file = file;
        this.infoMap = infoMap;
    }

    /*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");

            try (InputStream inStream = file.getInputStream(); DigestInputStream digestInputStream = new DigestInputStream(inStream, messageDigest)) {
                byte[] bytes;
                bytes = new byte[64*1024];
                while(digestInputStream.read(bytes) >= 0) {
                }
                String checksum = Hex.encodeHexString(digestInputStream.getMessageDigest().digest());
                infoMap.put("md5", checksum);
            } catch (IOException e) {
                e.printStackTrace(System.out);
                println("Error calculating digest for: " + file.getAbsolutePath() + " : " + e.getMessage());
            }
        } catch (NoSuchAlgorithmException nsae) {
            println("MD5 algorithm not found: " + nsae.getMessage());
        }
    }
}
