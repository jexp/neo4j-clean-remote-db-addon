package org.neo4j.server;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.extension.test.delete.LocalTestServer;

import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
        neoServer.cleanDb();
    }

    private GraphDatabaseAPI getGraphDb() {
        return neoServer.getDatabase().graph;
    }

    private long getNumberOfNodes(GraphDatabaseAPI graph) {
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
    public void multipleDeletesWithFewNodesDontDeleteDirectories() throws Exception {
        for (int i = 0; i < 10; i++) {
            createData(getGraphDb(), FEW_NODES);
            assertEquals(FEW_NODES + 1, getNumberOfNodes(getGraphDb()));
            ClientResponse response = Client.create().resource(createDeleteURI("secret-key")).delete(ClientResponse.class);
            final Map result = (Map) JSON.parse(response.getEntity(String.class));
            assertEquals(ClientResponse.Status.OK.getStatusCode(), response.getStatus());
            assertEquals(1, getNumberOfNodes(getGraphDb()));
            assertFalse(result.containsKey("store-dir"));
        }
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

    private void createData(GraphDatabaseAPI db, int max) {
        Transaction tx = db.beginTx();
        try {
            final IndexManager indexManager = db.index();
            Node[] nodes = new Node[max];
            for (int i = 0; i < max; i++) {
                nodes[i] = db.createNode();
                final Index<Node> index = indexManager.forNodes("node_index_" + String.valueOf(i % 5));
                index.add(nodes[i],"ID",i);
            }
            Random random = new Random();
            for (int i = 0; i < max * 2; i++) {
                int from = random.nextInt(max);
                final int to = (from + 1 + random.nextInt(max - 1)) % max;
                final Relationship relationship = nodes[from].createRelationshipTo(nodes[to], DynamicRelationshipType.withName("TEST_" + i));
                final Index<Relationship> index = indexManager.forRelationships("rel_index_" + String.valueOf(i % 5));
                index.add(relationship, "ID", i);
            }
            tx.success();
        } finally {
            tx.finish();
        }
    }
}
