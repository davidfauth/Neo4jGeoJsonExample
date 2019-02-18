package com.healthcaredemo.locator;

import java.io.IOException;
import java.io.OutputStream;

import java.util.Iterator;
import java.util.Map;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
 

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.logging.Log;
import org.neo4j.string.UTF8;

import static java.lang.String.format;

@Path("locator")
public class LocatorService {
	
	
  private static int LIMIT = 100000;


  @GET
  @Path("/helloworld")
  public String helloWorld() {
      return "Hello World!";
  }
  
  @GET
  @Path("/provider/State/City/{stateCode}/{cityName}/{maxRows}")
  public Response providerGeo( final @PathParam("stateCode") String stateCode, @PathParam("cityName") String cityName, @PathParam("maxRows") String maxRows,  @Context GraphDatabaseService db)
  {
      final Map<String, Object> params = MapUtil.map( "stateCode", stateCode.toUpperCase() );
      params.put("cityName", cityName.toUpperCase());
      params.put("maxRows", Integer.parseInt(maxRows));
      StreamingOutput stream = new StreamingOutput()
      {
          @Override
          public void write( OutputStream os ) throws IOException, WebApplicationException
          {
        	  JsonGenerator generator = Json.createGenerator(os);
        	  generator
        	  .writeStartObject()
                .write("type","FeatureCollection")
                .writeStartArray("features");
                try ( Transaction tx = db.beginTx();
                    Result result = db.execute( statsState(), params ) )
                    {
                        while ( result.hasNext() )
                        {
                            Map<String,Object> row = result.next();
                            double rowLong = Double.parseDouble(row.get("longitude").toString());
                            double rowLat = Double.parseDouble(row.get("latitude").toString());
                            String practiceName = row.get("practiceName").toString();
                            String NPI = row.get("NPI").toString();
                    generator
                    .writeStartObject()
                        .write("type","Feature")
                        .writeStartObject("properties")
                            .write("name",practiceName)
                            .write("NPI",NPI)
                        .writeEnd()
                        .writeStartObject("geometry")
                            .write("type","Point")
                            .writeStartArray("coordinates")
                                .write(rowLong)
                                .write(rowLat)
                            .writeEnd()
                        .writeEnd()
                    .writeEnd();
                        }
                        tx.success();
                    }
        generator
                .writeEnd()
              .writeEnd();

     		generator.close();
          }
      };

      return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
  }

  @GET
  @Path("/provider/Location/Taxonomy/{stateCode}/{cityName}/{taxonomy}/{maxRows}")
  public Response providerTaxonomyGeo( final @PathParam("stateCode") String stateCode, @PathParam("cityName") String cityName, @PathParam("taxonomy") String taxonomy, @PathParam("maxRows") String maxRows,  @Context GraphDatabaseService db)
  {
      final Map<String, Object> params = MapUtil.map( "stateCode", stateCode.toUpperCase() );
      params.put("cityName", cityName.toUpperCase());
      params.put("taxonomyCode", taxonomy.toUpperCase());
      params.put("maxRows", Integer.parseInt(maxRows));
      StreamingOutput stream = new StreamingOutput()
      {
          @Override
          public void write( OutputStream os ) throws IOException, WebApplicationException
          {
        	  JsonGenerator generator = Json.createGenerator(os);
        	  generator
        	  .writeStartObject()
                .write("type","FeatureCollection")
                .writeStartArray("features");
                try ( Transaction tx = db.beginTx();
                    Result result = db.execute( statsTaxonomyCityState(), params ) )
                    {
                        while ( result.hasNext() )
                        {
                            Map<String,Object> row = result.next();
                            double rowLong = Double.parseDouble(row.get("longitude").toString());
                            double rowLat = Double.parseDouble(row.get("latitude").toString());
                            String practiceName = row.get("practiceName").toString();
                            String NPI = row.get("NPI").toString();
                            String code = row.get("taxonomyCode").toString();
                    generator
                    .writeStartObject()
                        .write("type","Feature")
                        .writeStartObject("properties")
                            .write("name",practiceName)
                            .write("NPI",NPI)
                            .write("TaxonomyCode",code)
                        .writeEnd()
                        .writeStartObject("geometry")
                            .write("type","Point")
                            .writeStartArray("coordinates")
                                .write(rowLong)
                                .write(rowLat)
                            .writeEnd()
                        .writeEnd()
                    .writeEnd();
                        }
                        tx.success();
                    }
        generator
                .writeEnd()
              .writeEnd();

     		generator.close();
          }
      };

      return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
  }

  @GET
  @Path("/provider/Location/Taxonomy/{stateCode}/{taxonomy}/{maxRows}")
  public Response providerTaxonomyByState( final @PathParam("stateCode") String stateCode, @PathParam("taxonomy") String taxonomy, @PathParam("maxRows") String maxRows,  @Context GraphDatabaseService db)
  {
      final Map<String, Object> params = MapUtil.map( "stateCode", stateCode.toUpperCase() );
      params.put("taxonomyCode", taxonomy.toUpperCase());
      params.put("maxRows", Integer.parseInt(maxRows));
      StreamingOutput stream = new StreamingOutput()
      {
          @Override
          public void write( OutputStream os ) throws IOException, WebApplicationException
          {
        	  JsonGenerator generator = Json.createGenerator(os);
        	  generator
        	  .writeStartObject()
                .write("type","FeatureCollection")
                .writeStartArray("features");
                try ( Transaction tx = db.beginTx();
                    Result result = db.execute( statsTaxonomyState(), params ) )
                    {
                        while ( result.hasNext() )
                        {
                            Map<String,Object> row = result.next();
                            double rowLong = Double.parseDouble(row.get("longitude").toString());
                            double rowLat = Double.parseDouble(row.get("latitude").toString());
                            String practiceName = row.get("practiceName").toString();
                            String NPI = row.get("NPI").toString();
                            String code = row.get("taxonomyCode").toString();
                    generator
                    .writeStartObject()
                        .write("type","Feature")
                        .writeStartObject("properties")
                            .write("name",practiceName)
                            .write("NPI",NPI)
                            .write("TaxonomyCode",code)
                        .writeEnd()
                        .writeStartObject("geometry")
                            .write("type","Point")
                            .writeStartArray("coordinates")
                                .write(rowLong)
                                .write(rowLat)
                            .writeEnd()
                        .writeEnd()
                    .writeEnd();
                        }
                        tx.success();
                    }
        generator
                .writeEnd()
              .writeEnd();

     		generator.close();
          }
      };

      return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
  }

  @GET
  @Path("/provider/Location/Polygon/{taxonomy}/{maxRows}")
  public Response providerTaxonomyByPolygon( final @PathParam("taxonomy") String taxonomy, @PathParam("maxRows") String maxRows,  @Context GraphDatabaseService db)
  {
      final Map<String, Object> params = MapUtil.map( "taxonomyCode", taxonomy.toUpperCase() );
      params.put("maxRows", Integer.parseInt(maxRows));
      StreamingOutput stream = new StreamingOutput()
      {
          @Override
          public void write( OutputStream os ) throws IOException, WebApplicationException
          {
        	  JsonGenerator generator = Json.createGenerator(os);
        	  generator
        	  .writeStartObject()
                .write("type","FeatureCollection")
                .writeStartArray("features");
                try ( Transaction tx = db.beginTx();
                    Result result = db.execute( providerPolygonQuery(), params ) )
                    {
                        while ( result.hasNext() )
                        {
                            Map<String,Object> row = result.next();
                            double rowLong = Double.parseDouble(row.get("longitude").toString());
                            double rowLat = Double.parseDouble(row.get("latitude").toString());
                            String practiceName = row.get("practiceName").toString();
                            String NPI = row.get("NPI").toString();
                            String code = row.get("taxonomyCode").toString();
                    generator
                    .writeStartObject()
                        .write("type","Feature")
                        .writeStartObject("properties")
                            .write("name",practiceName)
                            .write("NPI",NPI)
                            .write("TaxonomyCode",code)
                        .writeEnd()
                        .writeStartObject("geometry")
                            .write("type","Point")
                            .writeStartArray("coordinates")
                                .write(rowLong)
                                .write(rowLat)
                            .writeEnd()
                        .writeEnd()
                    .writeEnd();
                        }
                        tx.success();
                    }
        generator
                .writeEnd()
              .writeEnd();

     		generator.close();
          }
      };

      return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
  }
  
  private String nearestProvider()
  {
      return "match (p:PostalCode {PostalCode: {postalCode}})-[h:HAS_DISTANCE]->(p1) where h.distance < 20 with p, collect(p1.PostalCode) as closeZips, point({latitude: p.latitude, longitude: p.longitude}) AS stafford match (a:Location) where a.PostalCode IN closeZips and exists(a.latitude) WITH a, distance(point(a), stafford) AS distance WHERE distance < 32000 WITH a, distance MATCH (t:TaxononmyCode {code: {taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a) WHERE p.LastName is not null RETURN t.Code,  p.LastName, p.NPI, a.Address1, a.CityName, a.StateName, apoc.number.format(distance/1600,'#,##0.00;(#,##0.00)') as milesAway, a.latitude, a.longitude ORDER BY distance LIMIT 100";
  }
  
  private String statsTaxonomyCityState()
  {
      //return "MATCH (n:Location)<-[:HAS_PRACTICE_AT]-(p:Provider)-[:HAS_SPECIALTY]->(t:TaxonomyCode) where exists(n.latitude) and n.StateName = {stateCode} and n.CityName = {cityName} and t.code = {taxonomyCode} return n.latitude as latitude, n.longitude as longitude, coalesce(p.BusinessName,p.FirstName + ' ' + p.LastName) as practiceName, p.NPI as NPI, t.code as taxonomyCode limit {maxRows}";
        return "MATCH (n:Location)<-[:HAS_PRACTICE_AT]-(p:Provider) where exists(n.latitude) and n.StateName = {stateCode} and n.CityName = {cityName} with distinct n,p match (p)-[:HAS_SPECIALTY]->(t:TaxonomyCode {code:{{taxonomyCode}}) return n.latitude as latitude, n.longitude as longitude, coalesce(p.BusinessName,p.FirstName + ' ' + p.LastName) as practiceName, p.NPI as NPI limit {maxRows}";
  }
  private String statsTaxonomyState()
  {
      //return "MATCH (n:Location)<-[:HAS_PRACTICE_AT]-(p:Provider)-[:HAS_SPECIALTY]->(t:TaxonomyCode) where exists(n.latitude) and n.StateName = {stateCode} and n.CityName = {cityName} and t.code = {taxonomyCode} return n.latitude as latitude, n.longitude as longitude, coalesce(p.BusinessName,p.FirstName + ' ' + p.LastName) as practiceName, p.NPI as NPI, t.code as taxonomyCode limit {maxRows}";
        return "MATCH (n:Location)<-[:HAS_PRACTICE_AT]-(p:Provider) where exists(n.latitude) and n.StateName = {stateCode} and n.CityName = {cityName} with distinct n,p match (p)-[:HAS_SPECIALTY]->(t:TaxonomyCode {code:{{taxonomyCode}}) return n.latitude as latitude, n.longitude as longitude, coalesce(p.BusinessName,p.FirstName + ' ' + p.LastName) as practiceName, p.NPI as NPI limit {maxRows}";
  }

  private String statsState()
  {
      return "MATCH (n:Location)<-[:HAS_PRACTICE_AT]-(p:Provider) where exists(n.latitude) and n.StateName = {stateCode} and n.CityName = {cityName} return n.latitude as latitude, n.longitude as longitude, coalesce(p.BusinessName,p.FirstName + ' ' + p.LastName) as practiceName, p.NPI as NPI limit {maxRows}";
  }
  
  private String statsStateByCounty()
  {
      return "match (t:TaxononmyCode {Code:{taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a:Location) where a.StateName = {stateCode} with a.Post5 as post5, t.Code as taxonomyCode, p match (p)<-[IP:IS_PATIENT]-() return  post5, taxonomyCode, max(IP.crowDistance) as maxCrow, min(IP.crowDistance) as minCrow, avg(IP.crowDistance) as avgCrow, count(IP) as numProviders order by post5 asc;";
  }
  
  private String statsPostalCode()
  {
      return "match (t:TaxononmyCode {Code:{taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a:Location) where a.Post5 = {postalCode} with a.Post5 as post5, t.Code as taxonomyCode, p match (p)<-[IP:IS_PATIENT]-() return post5, taxonomyCode, max(IP.crowDistance) as maxCrow, min(IP.crowDistance) as minCrow, avg(IP.crowDistance) as avgCrow, count(IP) as numProviders;";
  }
  
  private String statsCounty()
  {
      return "match (s:State {code:{stateCode}})<-[:IS_COUNTY_IN]-(:County {Name:{countyName}})<-[:IS_IN_COUNTY]-(pc:PostalCode) with collect(pc.PostalCode) as candidatePostalCodes match (t:TaxononmyCode {Code:{taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a:Location) where  a.Post5 IN candidatePostalCodes with a.Post5 as post5, t.Code as taxonomyCode, p match (p)<-[IP:IS_PATIENT]-() return post5, taxonomyCode, max(IP.crowDistance) as maxCrow, min(IP.crowDistance) as minCrow, avg(IP.crowDistance) as avgCrow, count(IP) as numProviders;";
  }
  
  private String providerPostalCodeQry()
  {
      return "match (p:PostalCode {PostalCode: {postalCode}})-[h:HAS_DISTANCE]->(p1) where h.Distance < 20 with p, collect(p1.PostalCode) as closeZips,point({latitude: p.latitude, longitude: p.longitude}) AS zipLocation match (a:Location) where a.PostalCode IN closeZips and exists(a.latitude) WITH a, distance(point(a), zipLocation) AS distance WHERE distance < 32000 WITH a, distance MATCH (t:TaxonomyCode {code:{taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a) WHERE p.LastName is not null RETURN t.code as taxonomyCode,  p.LastName as lastName, p.NPI as NPI, a.Address1 as address1, a.CityName as cityName, a.StateName as stateName, a.PostalCode as postalCode, a.Address1 + ' ' + a.CityName + ', ' + a.StateName + ' ' + a.PostalCode as fullAddress, distance/1600 as milesAway, a.latitude, a.longitude ORDER BY distance LIMIT 50;";
  }

  private String taxonomyByStateCodeQry()
  {
      return "match (t:TaxonomyCode {code:{taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a:Location) where a.StateName = {stateCode} with a.PostalCode as post5, t.code as taxonomyCode return post5, count(taxonomyCode) as providerCount order by providerCount DESC;";
  }

  private String providerBoundingBoxQuery()
  {
    return "MATCH (a:Location) where a.location < point({latitude: {startLatitude}, longitude: {startLongitude}}) and a.location > point({latitude: {endLatitude}, longitude: {endLongitude}}) with a limit 1000 MATCH (t:TaxonomyCode {code:{taxonomyCode}})<-[:HAS_SPECIALTY]-(p:Provider)-[:HAS_PRACTICE_AT]->(a) WHERE p.LastName is not null RETURN t.code as taxonomyCode,  p.LastName as lastName, p.NPI as NPI, a.Address1 as address1, a.CityName as cityName, a.StateName as stateName, a.PostalCode as postalCode, a.Address1 + ' ' + a.CityName + ', ' + a.StateName + ' ' + a.PostalCode as fullAddress, a.latitude, a.longitude LIMIT 50;";
  }

  private String pharmacyBoundingBoxQuery()
  {
    return "MATCH (a:Location) where a.location < point({latitude: {startLatitude}, longitude: {startLongitude}}) and a.location > point({latitude: {endLatitude}, longitude: {endLongitude}}) with a limit 1000 MATCH (p:Pharmacy)-[:IS_LOCATED_AT]->(a) return p.name as name, p.pMedcare as pMedCare, p.phone as phone, a.Address1 as address1, a.CityName as cityName, a.StateName as stateName, a.PostalCode as postalCode, a.Address1 + ' ' + a.CityName + ', ' + a.StateName + ' ' + a.PostalCode as fullAddress, a.latitude, a.longitude;";
  }

  private String providerPolygonQuery()
  {
    return "CALL com.dfauth.h3.polygonSearch([{lon:'-77.530886',lat:'38.3609911'},{lon:'-77.524492',lat:'38.3411698'},{lon:'-77.524492',lat:'38.277433'},{lon:'-77.5731773',lat:'38.2774607'},{lon:'-77.594635',lat:'38.2771873'}],[{}]) yield nodes unwind nodes as locNode match (locNode)<-[:HAS_BILLING_ADDRESS_AT]-(p:Provider)-[:HAS_SPECIALTY]->(t:TaxonomyCode {{taxonomyCode}}) return distinct t.code as taxonomyCode, locNode.latitude as latitude, locNode.longitude as longitude, coalesce(p.BusinessName,p.FirstName + ' ' + p.LastName) as practiceName, p.NPI as NPI limit {maxRows};";
  }
}
