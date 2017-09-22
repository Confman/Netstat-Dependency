package com.hakan

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.util.Calendar

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import scala.collection.{Map, mutable}
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext, graphx}

import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}

object SparkNetstat {
  def main(args: Array[String]) {

    val env = getEnv
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val serverPort = ConfigFactory.load().getString("my." + env + ".serverPort")
    val logFile = ConfigFactory.load().getString("my." + env + ".logFolder")

    val sparkBuilder = SparkSession.builder().appName("SparkSQL")
    if (env.equals("test")) {
      sparkBuilder.master("local[*]")
    }
    val spark = sparkBuilder.getOrCreate()

    val sc = spark.sparkContext
    var log = sc.parallelize(Seq(""))

    log = sc.textFile(serverPort + logFile)
    import spark.implicits._

    val p = new NetstatParser
    val parsedLines = log.map(line => p.parseRecord(line)).filter(_.isDefined)

    //test RDD (raw):
    printRDD(parsedLines)

    //possible filters:
    //val parsedLines_date = parsedLines.filter(x => p.filterDate(x, date))
    val parsedLine_httpResp = parsedLines.map(x => p.parseLine_NetstatTuple(x)).toDF("hostname", "ip_s", "ip_d", "port", "process")

    parsedLine_httpResp.createOrReplaceTempView("netstat")
    spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")

    val vertexMap = mutable.Map.empty[String,graphx.VertexId]
    val edgeMap = mutable.Map.empty[String,EdgeClass]

    val tableVert = spark.sql("select distinct ip_s, ip_d from netstat where ip_s<>ip_d order by ip_s, ip_d")
    val vertices = createGraphVertices(tableVert, 5000, sc, vertexMap)
    val tableEdge = spark.sql("select distinct ip_s, ip_d, port, process from netstat where ip_s<>ip_d order by ip_s, ip_d")
    val edges = createGraphEdges(tableEdge, 5000, sc, vertexMap, edgeMap)

    val graph = createGraph(vertices,edges,sc)
    createNeo4jGraph(env, graph)

    spark.stop()
  }

  def createGraphVertices (tableVert:DataFrame, rowCount:Int, sc:SparkContext, verticeMap:mutable.Map[String,graphx.VertexId]) = {

    val file = new File("public/debug_vertices.out");
    deleteRecursively(file);
    val writer = new PrintWriter(new FileWriter(file, true))

    writer.write("Starting debug \n")

    val results = tableVert.collect()

    var id=0L
    val vertIt = results.iterator
    while (vertIt.hasNext){

      val row = vertIt.next()
      val ip_s_pre = row.get(0).toString
      val ip_d_pre = row.get(1).toString
      if (!ip_s_pre.isEmpty && !ip_d_pre.isEmpty) {
        val ip_s = ip_s_pre + "_s"
        val ip_d = ip_d_pre + "_d"

        writer.write("row: "+row+" - ip_s: "+ip_s+ " - ip_d: "+ip_d+"\n")

        if (!verticeMap.get(ip_s).isDefined) {
          verticeMap.put(ip_s, id)
          id += 1;
        }
        if (!verticeMap.get(ip_d).isDefined) {
          verticeMap.put(ip_d, id)
          id += 1;
        }
      }
    }

    writer.close()

    val vertices = mutable.Map.empty[graphx.VertexId,String]
    val mapIt = verticeMap.iterator
    while (mapIt.hasNext){
      val item = mapIt.next()
      val ip = item._1
      val id = item._2

      vertices.put(id,ip)
    }
    vertices
  }

  //edge property class
  class EdgeClass {
    var edgeId = ""
    var processMap = mutable.Map.empty[String,ArrayBuffer[String]]
    var connCount = 0
  }

  def createGraphEdges (tableEdge:DataFrame, rowCount:Int, sc:SparkContext, verticeMap:mutable.Map[String,graphx.VertexId], edgeMap:mutable.Map[String,EdgeClass]) = {

    val results = tableEdge.collect()

    var edges = ArrayBuffer.empty[Edge[String]]
    val edgeIt = results.iterator
    while (edgeIt.hasNext){
      val row = edgeIt.next()
      val ip_s_pre = row.get(0).toString
      val ip_s    = ip_s_pre+"_s"
      val ip_d_pre = row.get(1).toString
      val ip_d    = ip_d_pre+"_d"
      val port    = row.get(2).toString
      val process = row.get(3).toString

      if (!ip_s_pre.equals(ip_d_pre)) {
        val s_id = verticeMap.get(ip_s)
        val d_id = verticeMap.get(ip_d)
        if (s_id.isDefined && d_id.isDefined) {
          val edgeId = ip_s+"-"+ip_d

          edgeMap.get(edgeId)
          //new edge
          if (!edgeMap.get(edgeId).isDefined) {
            val newEdge = new EdgeClass()
            newEdge.edgeId=edgeId
            var portList = ArrayBuffer.empty[String]
            portList += port
            newEdge.processMap.put(process,portList)
            newEdge.connCount += 1
            edgeMap.put(edgeId,newEdge)
          }
          else { //existing edge
            val existingEdgeObject = edgeMap.get(edgeId).get
            val existingprocessMap = existingEdgeObject.processMap

            //existing edge, new process
            if (!existingprocessMap.get(process).isDefined) {
              var portList = ArrayBuffer.empty[String]
              portList += port
              existingEdgeObject.processMap.put(process,portList)
              edgeMap.put(edgeId,existingEdgeObject)
            }
            //existing edge, existing process
            else {
              var existingPortList = existingprocessMap.get(process).get
              existingPortList += port
              existingprocessMap.put(process,existingPortList)
              existingEdgeObject.processMap = existingprocessMap
            }
            existingEdgeObject.connCount += 1
            edgeMap.put(edgeId,existingEdgeObject)
          }
        }
      }
    }

    val file = new File("public/debug_edges.out");
    deleteRecursively(file);
    val writer = new PrintWriter(new FileWriter(file, true))

    writer.write("Starting debug \n")

    val mapIt = edgeMap.iterator
    while (mapIt.hasNext){
      val item = mapIt.next()
      val edgeId = item._1
      val edgeObj = item._2

      var edgeProp = ""
      val edgeIt = edgeObj.processMap.iterator
      val edgeConnCount = edgeObj.connCount
      while (edgeIt.hasNext) {
        val item = edgeIt.next()
        val edgeProcess = item._1
        val edgeProcessPortList = item._2

        edgeProp += edgeProcess + "("+edgeProcessPortList.mkString(",")+"),"
      }
      edgeProp=edgeProp.substring(0,edgeProp.length()-1) //remove latest ,
      edgeProp+="-"+edgeConnCount

      val array = edgeId.split("-")
      val ip_s = array(0)
      val ip_d = array(1)

      writer.write("ip_s: "+ip_s+ " - ip_d: "+ip_d+"\n")

      if (verticeMap.get(ip_s).isDefined&& verticeMap.get(ip_d).isDefined) {
        val s_id = verticeMap.get(ip_s).get
        val d_id = verticeMap.get(ip_d).get
        edges += Edge(s_id, d_id, edgeProp)
      }
    }

    writer.close()

    edges
  }


  def createGraph (vertices:mutable.Map[graphx.VertexId, String], edges:ArrayBuffer[Edge[String]], sc:SparkContext) = {
    // Defining a default vertex called none
    if(!vertices.isEmpty && !edges.isEmpty) {
      //output files
      val file = new File("public/result.out");
      val file_json = new File("public/result.json");
      val file_gexf = new File("public/result.gexf");
      val none = "none"
      val eRDD= sc.parallelize(edges)
      val vRDD= sc.parallelize(vertices.toSeq)
      val graph = Graph(vRDD,eRDD,none)
            
      val bw_json = new BufferedWriter(new FileWriter(file_json))
      bw_json.write(toSigmaJson(graph))
      bw_json.close()
      val bw_gexf = new BufferedWriter(new FileWriter(file_gexf))
      bw_gexf.write(toGexf(graph))
      bw_gexf.close()   
     
      deleteRecursively(file);
      graph.triplets.coalesce(1,true).saveAsTextFile("public/result.out")
      //graph.triplets.collect().foreach(println)
      graph
    }
    else {
      printf("Vertices and Edges are empty!")
      null
    }
  }

  def createNeo4jGraph[VD,ED](env:String, g:Graph[VD,ED]) = {

    val dateFormat = new java.text.SimpleDateFormat("dd-MM-yyyy")
    val cal = Calendar.getInstance()
    val today = cal.getTimeInMillis
    val date_today = dateFormat.format(today)

    println("Neo4j started: "+cal.getTime())

    val bolt_host = ConfigFactory.load().getString("my." + env + ".bolt_host")

    val driver = GraphDatabase.driver("bolt://"+bolt_host+"/7687", AuthTokens.basic("neo4j", "admin123"))
    val session = driver.session

    var vertId = 1
    var vertIteration = 1
    val result = g.vertices.sortBy(_._1, ascending=true).distinct().collect()
    val vertIt = result.iterator
    val script_insert = StringBuilder.newBuilder

    val verticesCount = result.length

    var groupSize = 100
    if (verticesCount<groupSize)
      groupSize = verticesCount

    while (vertIt.hasNext) {
      val row = vertIt.next()
      val id = row._1.toString()
      val ip_type = row._2.toString()
      val array = ip_type.split("_")
      val ip = array(0)
      val category_ = array(1)
      var category = "SOURCE"
      if (category_.equals("d")) {
        category = "TARGET"
      }

      script_insert.append(
        "merge (n_"+id+":Nodes {id: '"+id+"'})\n"+
        "set n_"+id+".ip='"+ip+"',n_"+id+".lastupdatetime='"+date_today+"',n_"+id+".timestamp="+today+",n_"+id+".category='"+category+"'\n"
      )

      if (vertId == groupSize) {
        session.run(script_insert.toString())
        println("Neo4j nodes iteration no: "+vertIteration+" are created: - " + cal.getTime())
        vertId=0
        vertIteration = vertIteration + 1
        script_insert.clear()
      }
      vertId = vertId + 1
    }

    val script_index = "CREATE INDEX ON :Nodes(id)"
    session.run(script_index.toString())

    var edgeId = 1
    var edgeIdTotal = 1
    var edgeIteration = 1
    val result_edge = g.edges.distinct().collect()
    val edgeIt = result_edge.iterator

    val edgesCount = result_edge.length
    groupSize = 100
    if (edgesCount<groupSize)
      groupSize = verticesCount

    while (edgeIt.hasNext) {
      //sigma-related remediations:
      val row = edgeIt.next()
      val s_id = row.srcId.toString()
      val t_id = row.dstId.toString()
      val attr = row.attr.toString()

      //if (attr.length()>30)
      //  attr=attr.substring(0,30)+"..."

      val script_insert_rel = StringBuilder.newBuilder
      script_insert_rel.append(
        "MERGE (n_"+s_id+":Nodes {id: \""+s_id+"\"})\n"+
          "MERGE (n_"+t_id+":Nodes {id: \""+t_id+"\"})\n"+
          "MERGE (n_"+s_id+")-[r_"+edgeIdTotal+":CONNECTS_TO]->(n_"+t_id+") SET r_"+edgeIdTotal+".type = '"+attr+"'\n"
      )
      //println(script_insert_rel.toString())
      session.run(script_insert_rel.toString())

      if (edgeId == groupSize) {
        session.run(script_insert_rel.toString())
        println("Neo4j edges iteration no: "+edgeIteration+" are created: - " + cal.getTime())
        edgeId=0
        edgeIteration = edgeIteration + 1
        script_insert_rel.clear()
      }
      edgeId = edgeId + 1
      edgeIdTotal = edgeIdTotal + 1
    }

    //Query:
    //val script = "MATCH (n:Nodes) RETURN n"
    //val result = session.run(script)
    //println(result.list().size())

    //retention
    cal.add(Calendar.DATE, -30)
    val previousMonth = cal.getTimeInMillis
    val retention_script=
      "WITH "+previousMonth+" AS timestamp \n"+
        "MATCH (n:Nodes) \n"+
        "WHERE n.timestamp <= timestamp \n"+
        "DETACH DELETE n;"

    session.run(retention_script.toString())

    session.close()
    driver.close();
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  def printRDD (parsedLines: RDD[Option[NetstatRecord]]) = {

    val file = new File("public/debug_rdd.out");
    deleteRecursively(file);
    val writer = new PrintWriter(new FileWriter(file, true))
    val results = parsedLines.collect()
    for (result <- results) {
      if (result.isDefined) {
        writer.write(result.toString+"\n")
        //println("printRDD " + result)
      }

    }
    writer.close()
  }

  def toGexf[VD,ED](g:Graph[VD,ED]) : String = {
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "    <nodes>\n" +
      g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      "    </nodes>\n" +
      "    <edges>\n" +
      g.edges.map(e => "      <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "    </edges>\n" +
      "  </graph>\n" +
      "</gexf>"
  }



  def toSigmaJson[VD,ED](g:Graph[VD,ED]) : String = {

    val random_x = scala.util.Random
    val random_y = scala.util.Random

    val node_str = StringBuilder.newBuilder
    node_str.append("{\"nodes\":[\n")

    val vertIt = g.vertices.collect().iterator
    while (vertIt.hasNext) {
      //sigma-related remediations:
      val row = vertIt.next()
      val id = row._1.toString()
      val ip_type = row._2.toString()
      val array = ip_type.split("_")
      val ip = array(0)
      val category_ = array(1)
      var category = "SOURCE"
      var color = "rgb(255,51,51)"
      if (category_.equals("d")) {
        category = "TARGET"
        color = "rgb(1,179,255)"
      }
      node_str.append("{\"id\":\""+id+"\",\"label\":\""+ip+"\",\"x\":"+(random_x.nextFloat()+3)+",\"y\":"+(random_y.nextFloat()+3)+",\"size\":\"55.5\",\"color\":\""+color+"\",\"category\":\""+category+"\",\"ip\":\""+ip+"\"},\n")
    }

    node_str.append("],\n")
    node_str.append("\"edges\":[\n")

    var edgeId = 1

    val edgetIt = g.edges.collect().iterator
    while (edgetIt.hasNext) {
      //sigma-related remediations:
      val row = edgetIt.next()
      val s_id = row.srcId.toString()
      val t_id = row.dstId.toString()
      var attr = row.attr.toString()

      if (attr.length()>30)
        attr=attr.substring(0,30)+"..."

      node_str.append("{\"id\":\""+edgeId+"\",\"label\":\""+attr+"\",\"source\":\""+s_id+"\",\"target\":\""+t_id+"\",\"color\":\"rgb(0,0,0)\"},\n")
      edgeId=edgeId+1
    }

    node_str.append("]}\n")
    var result = node_str.toString()
    result = result.replaceAll("},\n]","}\n]")
    result
  }


  def getEnv: String = {
    val osName = System.getProperty("os.name")
    if (osName.startsWith("Windows"))
      "test"
    else
      "prod"
  }

}
