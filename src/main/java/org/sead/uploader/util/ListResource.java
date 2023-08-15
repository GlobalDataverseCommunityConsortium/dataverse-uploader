/** *****************************************************************************
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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.http.entity.mime.content.ContentBody;
import org.json.JSONObject;

/**
 * A limited functionality class to collect any top-level individual files
 * listed on the command line. In the Dataverse implementation, we need to
 * post-process these files in some cases, e.g. with directupload and not doing
 * singlefile registration with Dataverse.
 *
 * @author qqmye
 */
public class ListResource extends Resource {

    private final ArrayList<Resource> fileResourceArray;
    private final String name;

    public ListResource(String name) {
        this.fileResourceArray = new ArrayList<>();
        this.name = name;
    }

    @Override
    public String getName() {

        return name;
    }

    @Override
    public boolean isDirectory() {
        return true;
    }

    @Override
    public String getPath() {
        return name;
    }

    @Override
    public String getMimeType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Resource> iterator() {
        return fileResourceArray.iterator();
    }

    @Override
    public Iterable<Resource> listResources() {
        return fileResourceArray;
    }

    public void addResource(Resource fr) {
        if (fr.isDirectory()) {
            throw new UnsupportedOperationException();
        } else {
            fileResourceArray.add(fr);
        }
    }

    @Override
    public long length() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getAbsolutePath() {
        return name;
    }

    @Override
    public ContentBody getContentBody() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getInputStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getHash(String algorithm) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JSONObject getMetadata() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMetadata(JSONObject jo) {
        throw new UnsupportedOperationException();
    }

}
