package org.neo4j.server.extension.test.delete;

/**
 * @author mh
 * @since 27.02.11
 */

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.domain.JsonHelper;

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
import java.util.Map;
import java.util.logging.Logger;

import static org.neo4j.server.configuration.Configurator.DATABASE_LOCATION_PROPERTY_KEY;

@Path("/")
public class DeleteDatabaseResource {

    private static final String CONFIG_DELETE_AUTH_KEY = "org.neo4j.server.thirdparty.delete.key";
    public static final long MAX_NODES_TO_DELETE = 1000;
    private final Database database;
    private Configuration config;
    private Logger log = Logger.getLogger(DeleteDatabaseResource.class.getName());

    public DeleteDatabaseResource(@Context Database database, @Context Configuration config) {
        this.database = database;
        this.config = config;
    }

    @DELETE
    @Path("/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cleanDb(@PathParam("key") String deleteKey) {
        GraphDatabaseAPI graph = database.getGraph();
        String configKey = config.getString(CONFIG_DELETE_AUTH_KEY);

        if (deleteKey == null || configKey == null || !deleteKey.equals(configKey)) {
            return Response.status(Status.UNAUTHORIZED).build();
        }
        try {
            Map<String, Object> result = new Neo4jDatabaseCleaner(graph).cleanDb(MAX_NODES_TO_DELETE);
            if ((Long)result.get("nodes")>=MAX_NODES_TO_DELETE) {
                result.putAll(cleanDbDirectory(database));
            }
            log.warning("Deleted Database: " + result);
            return Response.status(Status.OK).entity(JsonHelper.createJsonFrom(result)).build();
        } catch (Throwable e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity(JsonHelper.createJsonFrom(e.getMessage())).build();
        }
    }

    private Map<String, Object> cleanDbDirectory(Database database) throws Throwable {
        AbstractGraphDatabase graph = database.graph;
        String storeDir = graph.getStoreDir();
        if (storeDir == null) {
            storeDir = config.getString(DATABASE_LOCATION_PROPERTY_KEY);
        }
        graph.shutdown();
        Map<String, Object> result = removeDirectory(storeDir);

        // TODO wtf?
        // database.graph = new EmbeddedGraphDatabase(storeDir, graph.getKernelData().getConfigParams());
        database.start();

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
}
