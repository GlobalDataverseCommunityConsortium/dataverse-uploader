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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.input.BoundedInputStream;

import org.apache.http.entity.mime.content.ContentBody;
import org.json.JSONObject;

public abstract class Resource implements Iterable<Resource> {

    public abstract String getName();

    public abstract boolean isDirectory();

    public abstract String getPath();

    public abstract long length();

    public abstract String getAbsolutePath();

    public abstract ContentBody getContentBody();

    public abstract InputStream getInputStream();

    public abstract Iterable<Resource> listResources();

    public abstract String getHash(String algorithm);

    public abstract JSONObject getMetadata();

    public abstract String getMimeType();

    /**
     *
     * @param l
     * @param partSize
     * @return
     */
    public InputStream getInputStream(long l, long partSize) {
        try {
            InputStream is = getInputStream();
            is.skip(l);
            return new BoundedInputStream(is, partSize);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
