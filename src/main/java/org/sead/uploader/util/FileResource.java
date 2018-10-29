/*******************************************************************************
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
 ******************************************************************************/

package org.sead.uploader.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.FileBody;
import org.json.JSONObject;


public class FileResource implements Resource {

	private File f;

	public FileResource(String fileName) {
		f = new File(fileName);
	}

	private FileResource(File file) {
		f = file;
	}

	@Override
	public String getName() {

		return f.getName();
	}

	@Override
	public boolean isDirectory() {
		return f.isDirectory();
	}

	@Override
	public String getPath() {
		return f.getPath();
	}

	@Override
	public Iterator<Resource> iterator() {
		return listResources().iterator();
	}

	@Override
	public Iterable<Resource> listResources() {
		ArrayList<Resource> resources = new ArrayList<Resource>();
		for (File file : f.listFiles()) {
			resources.add(new FileResource(file));
		}
		return resources;
	}

	@Override
	public long length() {
		return f.length();
	}

	@Override
	public String getAbsolutePath() {
		return f.getAbsolutePath();
	}

	@Override
	public ContentBody getContentBody() {
		ContentType cType = ContentType.DEFAULT_BINARY;
		try {
			String mType = Files.probeContentType(f.toPath());
			
			if(mType!= null) {
				cType = ContentType.create(mType);
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return new FileBody(f, cType , f.getName());
	}

	@Override
	public String getHash(String algorithm) {
        MessageDigest digester = null;
        InputStream is=null;
		try {
			is = new FileInputStream(f);
            digester = MessageDigest.getInstance(algorithm);
            is = new DigestInputStream(is, digester);
            byte[] b = new byte[8192];
            while(is.read(b)!=-1);
            byte[] digest = digester.digest();
            return Hex.encodeHexString(digest);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}         catch (NoSuchAlgorithmException e1) {
            e1.printStackTrace();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			IOUtils.closeQuietly(is);
		}
		return null;
	}

	@Override
	public JSONObject getMetadata() {
		//No extra metadata by default for files
		return new JSONObject();
	}

}
