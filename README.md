Just put the [test-delete-db-extension-VERSION.jar](https://github.com/jexp/neo4j-clean-remote-db-addon/archives/master) for your Neo4j-VERSION into neo4j-server/plugins and add this to the server properties file

    org.neo4j.server.thirdparty_jaxrs_classes=org.neo4j.server.extension.test.delete=/db/data/cleandb
    org.neo4j.server.thirdparty.delete.key=secret-key


Then you can issue

    curl -X DELETE http://localhost:7474/db/data/cleandb/secret-key


to delete the graph database without restarting the server
