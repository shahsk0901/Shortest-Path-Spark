package edu.uta.cse6331

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.Map

object Source {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Single Source Shortest Path")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val readInput = sc.textFile(args(0))

    val inputGraph = readInput.map(line => {
      val processLine = line.split(",")
      (processLine(0).toLong, processLine(1).toLong, processLine(2).toLong)
    })

    // Grouping to avoid duplicate entries in initializing variables
    val inputGraphGroupedByDestination = inputGraph.groupBy(_._3)

    // Initializing the destination distances with Max value
    var updateDestinationDistance = inputGraphGroupedByDestination.map(dist => {
      if (dist._1 == 0) {
        (dist._1, 0.toLong)
      } else {
        (dist._1, Long.MaxValue)
      }
    })

    // Using another RDD to update the source distance values and maintain
    // distance vector for both i & j
    var updateDistanceVector = inputGraph.map(updateSource => {
      (Long.MaxValue, updateSource._1, updateSource._2, updateSource._3)
    })

    for (i <- 1 until 5) {
      
      updateDistanceVector = updateDistanceVector.map(updateDistanceVector => {
        (updateDistanceVector._4, updateDistanceVector)
      }).join(
        updateDestinationDistance.map(updateDestinationDistance => {
          (updateDestinationDistance._1,updateDestinationDistance)
        })
      ).map { case (destination, (updateDistanceVector,updateDestinationDistance))
        => (updateDestinationDistance._2,updateDistanceVector._1,
            updateDistanceVector._2,updateDistanceVector._3)
      }
      
      updateDestinationDistance = updateDistanceVector.map(updateDistanceVector => {
        (updateDistanceVector._2,updateDistanceVector)
      }).join(
        updateDestinationDistance.map(updateDestinationDistance => {
          (updateDestinationDistance._1,updateDestinationDistance)
        })    
      ).map { case (destination, (updateDistanceVector,updateDestinationDistance))
        => (updateDistanceVector._4,if(
            (updateDistanceVector._1 > (updateDestinationDistance._2 + updateDistanceVector._3)) &&
            (updateDistanceVector._1 != Long.MaxValue)
            ) {
          (updateDestinationDistance._2 + updateDistanceVector._3)
        } else {
          (updateDistanceVector._1)
        }
        )}.reduceByKey(_ min _)
        
     }
    
    updateDestinationDistance = updateDestinationDistance.sortBy(_._1)
    updateDestinationDistance = updateDestinationDistance.filter(updateDestinationDistance => (updateDestinationDistance._2 != Long.MaxValue))
    
    updateDestinationDistance.collect.foreach(println)
  }
}