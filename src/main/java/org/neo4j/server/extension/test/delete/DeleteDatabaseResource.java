package org.neo4j.server.extension.test.delete;

/**
 * @author mh
 * @since 27.02.11
 */

import com.sun.jersey.api.core.ResourceConfig;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.neo4j.graphdb.*;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

@Path("/")
public class DeleteDatabaseResource {

    private static final String CONFIG_DELETE_AUTH_KEY = "org.neo4j.server.thirdparty.delete.key";
    private final Database database;
    private ResourceConfig resourceConfig;
    private Configuration config;
    private Logger log = Logger.getLogger(DeleteDatabaseResource.class.getName());

    public DeleteDatabaseResource(@Context Database database, @Context ResourceConfig resourceConfig, @Context Configuration config) {
        this.database = database;
        this.resourceConfig = resourceConfig;
        this.config = config;
    }

    @DELETE
    @Path("/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cleanDb(@PathParam("key") String deleteKey) {
        AbstractGraphDatabase graph = database.graph;
        String configKey = config.getString(CONFIG_DELETE_AUTH_KEY);

        if (deleteKey == null || configKey == null || !deleteKey.equals(configKey)) {
            return Response.status(Status.UNAUTHORIZED).build();
        }
        try {
            long nodes = getNumberOfNodes(graph);
            Map<String, Object> result = nodes > 1000 ? cleanDbDirectory(database) : new Neo4jDatabaseCleaner(graph).cleanDb();
            log.warning("Deleted Database: " + result);
            return Response.status(Status.OK).entity(JSONObject.toJSONString(result)).build();
        } catch (Exception e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity(JSONValue.toJSONString(e.getMessage())).build();
        }
    }

    private long getNumberOfNodes(AbstractGraphDatabase graph) {
        return graph.getConfig().getGraphDbModule().getNodeManager().getNumberOfIdsInUse(Node.class);
    }

    private Map<String, Object> cleanDbDirectory(Database database) throws IOException {
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
}
