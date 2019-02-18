# Neo4j Unmanaged Extension - Output GeoJSON

Example of returning streaming GeoJSON from our healthcare demo data set 

# Instructions

1. Build it:

        mvn clean package

2. Copy target/finder-0.1-SNAPSHOT.jar to the plugins/ directory of your Neo4j server.

3. Configure Neo4j by adding a line to conf/neo4j.conf:

        dbms.unmanaged_extension_classes=com.healthcaredemo=/v1

4. Start Neo4j server.

5. Query the Neo4j server:
		
		# Providers by Cit, State Code, Taxonomy Code and max rows
        :GET /v1/provider/Location/Taxonomy/VA/Stafford/207Q00000X/100")

		# Providers by State Code, Taxonomy Code and max rows
        :GET /v1/provider/Location/Taxonomy/VA/207Q00000X/100

6. TO DO
		Pass in polygon as parameter