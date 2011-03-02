Just put the [jar](https
://github.com/jexp/neo4j-clean-remote-db-addon/blob/master/dist/test-delete-db-extension-1.0-SNAPSHOT.jar?raw=true) (dist/test-delete-db-extension-1.0-SNAPSHOT.jar) into neo4j-server/lib and add this to the server properties file

    org.neo4j.server.thirdparty_jaxrs_classes=org.neo4j.server.extension.test.delete=/cleandb
    org.neo4j.server.thirdparty.delete.key=secret-key


Then you can issue

    curl -X DELETE http://localhost:7474/cleandb/secret-key


to delete the graph database w/o restarting the server
