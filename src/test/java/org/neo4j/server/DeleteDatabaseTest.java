package org.neo4j.server;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.extension.test.delete.LocalTestServer;
import org.neo4j.server.extension.test.delete.Neo4jDatabaseCleaner;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.ThirdPartyJAXRSModule;
import org.neo4j.server.startup.healthcheck.StartupHealthCheck;
import org.neo4j.server.web.Jetty6WebServer;

import java.io.File;
import java.net.URL;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 02.03.11
 */
public class DeleteDatabaseTest {
    private static LocalTestServer neoServer = new LocalTestServer().withPropertiesFile("test-db.properties");

    private static final int MANY_NODES = 1500;
    private static final int FEW_NODES = 500;

    private static final String CONTEXT_PATH = "cleandb";

    @BeforeClass
    public static void startServerWithACleanDb() {
        neoServer.start();
    }

    @AfterClass
    public static void shutdownServer() {
        neoServer.stop();
    }

    @Before
    public void cleanDb() {
        new Neo4jDatabaseCleaner(getGraphDb()).cleanDb();
    }

    private AbstractGraphDatabase getGraphDb() {
        return neoServer.getDatabase().graph;
    }

    private long getNumberOfNodes(AbstractGraphDatabase graph) {
        long count=0;
        for (Node node : graph.getAllNodes()) {
            count++;
        }
        return count;
        //return graph.getConfig().getGraphDbModule().getNodeManager().getNumberOfIdsInUse(Node.class);
    }

    @Test
    public void deleteWithWrongKey() throws Exception {
        ClientResponse response = Client.create().resource(createDeleteURI("wrong-key")).delete(ClientResponse.class);
        assertEquals(ClientResponse.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
    }

    @Test
    public void deleteWithFewNodes() throws Exception {
        createData(getGraphDb(), FEW_NODES);
        assertEquals(FEW_NODES +1, getNumberOfNodes(getGraphDb()));
        ClientResponse response = Client.create().resource(createDeleteURI("secret-key")).delete(ClientResponse.class);
        assertEquals(ClientResponse.Status.OK.getStatusCode(), response.getStatus());
        assertEquals(1, getNumberOfNodes(getGraphDb()));
    }
    @Test
    public void deleteWithManyNodes() throws Exception {
        createData(getGraphDb(), MANY_NODES);
        assertEquals(MANY_NODES+1, getNumberOfNodes(getGraphDb()));
        ClientResponse response = Client.create().resource(createDeleteURI("secret-key")).delete(ClientResponse.class);
        assertEquals(ClientResponse.Status.OK.getStatusCode(), response.getStatus());
        assertEquals(1, getNumberOfNodes(getGraphDb()));
    }

    private String createDeleteURI(String key) {
        return String.format(neoServer.baseUri().toString()  + "%s/%s", CONTEXT_PATH, key);
    }

    private void createData(AbstractGraphDatabase db, int max) {
        Transaction tx = db.beginTx();
        try {
            Node[] nodes = new Node[max];
            for (int i = 0; i < max; i++) {
                nodes[i] = db.createNode();
            }
            Random random = new Random();
            for (int i = 0; i < max * 2; i++) {
                int index = random.nextInt(max);
                Node n1 = nodes[index];
                Node n2 = nodes[(index + 1 + random.nextInt(max - 1)) % max];
                n1.createRelationshipTo(n2, DynamicRelationshipType.withName("TEST_" + i));
            }
            tx.success();
        } finally {
            tx.finish();
        }
    }
}
