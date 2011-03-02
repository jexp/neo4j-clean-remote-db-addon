package org.neo4j.server.extension.test.delete;

/**
 * @author mh
 * @since 27.02.11
 */

import com.sun.jersey.api.core.ResourceConfig;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.server.database.Database;

import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

@Path("/")
public class DeleteDatabaseResource {

    private final Database database;
    private ResourceConfig config;
    private Logger log = Logger.getLogger(DeleteDatabaseResource.class.getName());

    public DeleteDatabaseResource(@Context Database database, @Context ResourceConfig config) {
        this.database = database;
        this.config = config;
    }

    @DELETE
    @Path("/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response hello(@PathParam("key") String deleteKey) {
        AbstractGraphDatabase graph = database.graph;
        Object configKey = graph.getConfig().getParams().get("delete-key");
        if (deleteKey == null || configKey == null || !deleteKey.equals(configKey)) {
            return Response.status(Status.UNAUTHORIZED).build();
        }
        try {
            Map<String, Object> result = cleanDbDirectory();
            log.warning("Deleted Database: " + result);
            return Response.status(Status.OK).entity(JSONObject.toJSONString(result)).build();
        } catch (Exception e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity(JSONValue.toJSONString(e.getMessage())).build();
        }
    }

    private Map<String, Object> cleanDbDirectory() throws IOException {
        String storeDir = database.graph.getStoreDir();
        Map params = getAndFilterParams();
        database.shutdown();

        Map<String, Object> result = removeDirectory(storeDir);

        database.graph = new EmbeddedGraphDatabase(storeDir,params);

        return result;
    }

    private Map<String, Object> removeDirectory(String storeDir) throws IOException {
        File dir = new File(storeDir);
        Map<String,Object> result=new HashMap<String, Object>();
        result.put("store-dir",dir);
        result.put("size", FileUtils.sizeOfDirectory(dir));
        FileUtils.deleteDirectory(dir);
        return result;
    }

    private Map getAndFilterParams() {
        Map params = database.graph.getConfig().getParams();
        for (Iterator param = params.entrySet().iterator(); param.hasNext();) {
            Map.Entry entry = (Map.Entry) param.next();
            Object value = entry.getValue();
            if (entry.getKey() instanceof String && (value instanceof String || value instanceof Number || value instanceof Boolean)) {
                continue;
            }
            param.remove();
        }
        System.out.println("params = " + params);
        return params;
    }

    public Map<String, Object> cleanDb(GraphDatabaseService graphDatabaseService) {
        Map<String, Object> result = new HashMap<String, Object>();
        Transaction tx = graphDatabaseService.beginTx();
        try {
            removeNodes(graphDatabaseService, result);
            clearIndex(graphDatabaseService, result);
            tx.success();
        } finally {
            tx.finish();
        }
        return result;
    }

    private void removeNodes(GraphDatabaseService graphDatabaseService, Map<String, Object> result) {
        Node refNode = graphDatabaseService.getReferenceNode();
        int nodes = 0, relationships = 0;
        for (Node node : graphDatabaseService.getAllNodes()) {
            for (Relationship rel : node.getRelationships(Direction.OUTGOING)) {
                rel.delete();
                relationships++;
            }
            if (!refNode.equals(node)) {
                node.delete();
                nodes++;
            }
        }
        result.put("nodes", nodes);
        result.put("relationships", relationships);

    }

    private void clearIndex(GraphDatabaseService gds, Map<String, Object> result) {
        IndexManager indexManager = gds.index();
        result.put("node-indexes", Arrays.asList(indexManager.nodeIndexNames()));
        result.put("relationship-indexes", Arrays.asList(indexManager.relationshipIndexNames()));
        for (String ix : indexManager.nodeIndexNames()) {
            indexManager.forNodes(ix).delete();
        }
        for (String ix : indexManager.relationshipIndexNames()) {
            indexManager.forRelationships(ix).delete();
        }
    }

}
