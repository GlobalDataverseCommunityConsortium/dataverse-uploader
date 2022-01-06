/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sead.uploader.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.json.JSONObject;

/**
 *
 * @author qqmye
 */
public class BagResourceFactory extends ResourceFactory {

    private ZipFile zf = null;
    Map<String, String> pidMap = new HashMap<String, String>();

    private String basePath;
    /**
     *
     * @param bagUrl
     */
    public BagResourceFactory(URL bagUrl) {
        try {
            File result = new File(bagUrl.toURI());

            zf = new ZipFile(result);
            List<String> paths = new ArrayList<String>();
            Enumeration<? extends ZipEntry> entries = zf.entries();
            while (entries.hasMoreElements()) {
                ZipEntry ze = entries.nextElement();
                if (ze.isDirectory()) {
                    paths.add(ze.getName());
                }
            }
            System.out.println(String.join(", ",paths));
            basePath = paths.get(0);
            basePath = basePath.substring(0, basePath.indexOf("/"));
            int dataIndex = paths.indexOf(basePath + "/data");
            String nextDir = paths.get(dataIndex + 1);
            //check if dir too
            //nextDir.endsWith(server)

            ZipEntry pidMapEntry = zf.getEntry(findPidMapFile());
            ZipEntry oreMapEntry = zf.getEntry(findOreMapFile());
            String mapString = new BufferedReader(
                    new InputStreamReader(zf.getInputStream(oreMapEntry), UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));
            createOreMapFromString(mapString);
            String datasetTitle = oremap.getJSONObject("describes").getString("Title");
            if(nextDir.equals(basePath + "/data/" + datasetTitle) && zf.getEntry(nextDir).isDirectory()) {
                //Previous default
                rootPath=nextDir;
            } else {
                //New default for RDA-compliant Bags
                rootPath = basePath + "/data";
            }
            createPidMap(new BufferedReader(new InputStreamReader(zf.getInputStream(pidMapEntry), UTF_8)));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public Resource getPublishedResource(String id, String parentPath) {
        int i = index.indexOf(id);
        PublishedResource p = new BagResource(
                aggregates.getJSONObject(i - 1));
        String path = parentPath + "/" + p.getName();
        p.setAbsolutePath(rootPath + "/" + path);
        p.setPath(path);
        return p;
    }

    public PublishedResource getParentResource() {
        JSONObject agg = oremap.getJSONObject("describes");
        agg.remove("aggregates");
        PublishedResource p = new BagResource(agg);
        p.setAbsolutePath(rootPath);
        p.setPath(p.getName());
        return p;
    }

    private void createPidMap(BufferedReader pidReader) {
        pidReader.lines().forEach(line -> {
            int index = line.indexOf(" ");
            pidMap.put(line.substring(0, index), line.substring(index + 1));
            System.out.println(line.substring(0, index)+ ": "  + line.substring(index + 1));
        });
    }
    
    public ZipFile getZipFile() {
        return zf;
    }
    
    public String getBasePath() {
        return basePath;
    }

    private String findPidMapFile() {
        //ToDo - can look for file at other paths, or generate the same info from other sources
        return basePath + "/metadata/pid-mapping.txt";
    }

    private String findOreMapFile() {
        //ToDo - can look at other paths
        return basePath + "/metadata/oai-ore.jsonld";
    }

}
