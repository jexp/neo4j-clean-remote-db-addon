Just put the jar in neo4j-server/lib and add this to the server properties file

    org.neo4j.server.thirdparty_jaxrs_classes=org.neo4j.server.extension.test.delete=/test
    org.neo4j.server.thirdparty.delete.key=secret-key


Then you can issue

    curl -X DELETE http://localhost:7474/test/secret-key


to delete the graph database w/o restarting the server
