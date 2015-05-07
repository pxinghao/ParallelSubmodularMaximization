/*
* Copyright 2015 [See AUTHORS file for list of authors]
*
*    Licensed under the Apache License, Version 2.0 (the "License");
*    you may not use this file except in compliance with the License.
*    You may obtain a copy of the License at
*
*        http://www.apache.org/licenses/LICENSE-2.0
*
*    Unless required by applicable law or agreed to in writing, software
*    distributed under the License is distributed on an "AS IS" BASIS,
*    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*    See the License for the specific language governing permissions and
*    limitations under the License.
*/

import java.util.Random
import java.util.concurrent.{Executors, Callable}

import it.unimi.dsi.webgraph.BVGraph

import collection.JavaConversions._

/**
 * A couple of examples (set cover and max graph cut) demonstrating CC-2g and CF-2g.
 */
object Examples{

  /**
   * A serial Knuth shuffle generating uniform random permutation
   * @param array Array of elements to be shuffled
   * @param rnd   Random object used for random shuffling
   * @tparam T    Class type of array
   * @return      Shuffled array
   */
  protected def shuffle[T](array: Array[T], rnd: Random): Array[T] = {
    for (n <- Iterator.range(array.length - 1, 0, -1)) {
      val k = rnd.nextInt(n + 1)
      val t = array(k); array(k) = array(n); array(n) = t
    }
    array
  }

  /**
   * Reads a BVGraph from file into SparseGraph format
   * @param filename Filename (not including extension) of BVGraph to read
   * @return         SparseGraph of BVGraph read from disk
   */
  protected def readGraphFromBVGraph(filename: String) : SparseGraph = {
    val graph = new SparseGraph
    graph.loadFromBVGraph(BVGraph.load(filename))
    graph
  }

  /**
   * Driver method (main class) for choosing either set cover / max graph cut
   * @param args Command line arguments
   */
  def main(args: Array[String]) = {

    val argmap = args.map { a =>
      val argPair = a.split("=")
      val name = argPair(0).toLowerCase
      val value = argPair(1)
      (name, value)
    }.toMap

    // Default options
    val inputFile       = argmap.getOrElse("inputfile",  "data/eswiki-2013_rand_symm")
    val randSeed        = argmap.getOrElse("randseed",   "43").toInt
    val maxNThreads     = argmap.getOrElse("maxnthread", "4").toInt
    val objectiveType   = argmap.getOrElse("objectivetype", "1").toInt
    val lambda          = argmap.getOrElse("lambda", "1.0").toDouble

    println(s"Input dataset $inputFile for problem ${if (objectiveType==0) "max graph cut" else "set cover"}")

    if (objectiveType == 0){
      // Max graph cut
      main_maxgraphcut(randSeed, maxNThreads, inputFile)
    }else{
      // Set cover
      main_setcover(randSeed, maxNThreads, lambda, inputFile)
    }
  }

  /**
   * Example of maximizing set cover objective function
   * @param randSeed    Random seed
   * @param maxNThreads Maximum number of threads to run CC-2g / CF-2g set cover on
   * @param lambda      Weight of cardinality term in objective function
   * @param inputFile   Input BVGraph filename
   */
  def main_setcover(randSeed: Int, maxNThreads: Int, lambda: Double, inputFile: String) = {

    val gen = new Random(randSeed)

    val graph = readGraphFromBVGraph(inputFile)
    val nElements = graph.numNodes()
    val nGroups = nElements

    System.gc()

    // Draw uniform random numbers
    val u = new Array[Double](nElements)
    val uniformSampleSeeds = (0 until maxNThreads).map(_ => gen.nextInt()).toArray
    val uniformSampleTasks = (0 until maxNThreads).map(threadId => new Callable[Unit]{
      override def call() = {
        try{
          val localGen = new Random(uniformSampleSeeds(threadId))
          var i = threadId
          while (i < nElements){
            u(i) = localGen.nextDouble()
            i += maxNThreads
          }
        }catch{
          case e: Exception => e.printStackTrace()
        }
      }
    }).toList
    val randThreadPool = Executors.newFixedThreadPool(maxNThreads)
    randThreadPool.invokeAll(uniformSampleTasks)
    randThreadPool.shutdown()



    for (nThreads <- 0 to maxNThreads){
      if (nThreads == 0){

        val serlSetCover = new Sequential_SetCover(nElements, nGroups, graph, lambda)

        System.gc()
        val serlStartTime_iter = System.currentTimeMillis()
        serlSetCover.run(43, u)
        val serlEndTime_iter = System.currentTimeMillis()
        val serlSetCoverValue = SetCoverFunctions.computeSetCover(nElements, nGroups, serlSetCover.A, lambda, graph, 32)

        val serlTime = serlEndTime_iter - serlStartTime_iter

        println(s"Serial (1 threads) completed in $serlTime ms with objective value $serlSetCoverValue")

      }else{
        val cc2gSetCover = new ConcurrencyControl_SetCover(nElements, nGroups, graph, lambda)
        val cf2gSetCover = new CoordinationFree_SetCover(nElements, nGroups, graph, lambda)

        val threads = Executors.newFixedThreadPool(nThreads)

        var cc2gTime : Long = 0
        var cc2gSetCoverValue = 0.0
        if (true){
          System.gc()
          val startTime = System.currentTimeMillis()
          cc2gSetCover.run(nThreads, threads, 43, u)
          val endTime = System.currentTimeMillis()
          cc2gSetCoverValue = SetCoverFunctions.computeSetCover(nElements, nGroups, cc2gSetCover.Ahat, lambda, cc2gSetCover.alphhat, 32)
          cc2gTime = endTime - startTime
        }

        println(s"CC-2g  ($nThreads threads) completed in $cc2gTime ms with objective value $cc2gSetCoverValue")

        var cf2gTime : Long = 0
        var cf2gSetCoverValue = 0.0
        if (true){
          System.gc()
          val startTime = System.currentTimeMillis()
          cf2gSetCover.run(nThreads, threads, 43, u)
          val endTime = System.currentTimeMillis()
          cf2gSetCoverValue = SetCoverFunctions.computeSetCover(nElements, nGroups, cf2gSetCover.Ahat, lambda, cf2gSetCover.alphhat, 32)
          cf2gTime = endTime - startTime
        }

        println(s"CF-2g  ($nThreads threads) completed in $cf2gTime ms with objective value $cf2gSetCoverValue")
        
        threads.shutdown()
      }
    }
  }

  /**
   * Example of maximizing max graph cut objective function
   * @param randSeed    Random seed
   * @param maxNThreads Maximum number of threads to run CC-2g / CF-2g max graph cut on
   * @param inputFile   Input BVGraph filename
   */
  def main_maxgraphcut(randSeed: Int, maxNThreads: Int, inputFile: String) = {

    val gen = new Random(randSeed)

    val graph = readGraphFromBVGraph(inputFile)
    val nVertices = graph.numNodes()

    System.gc()

    // Draw uniform random numbers
    val u = new Array[Double](nVertices)
    val uniformSampleSeeds = (0 until maxNThreads).map(_ => gen.nextInt()).toArray
    val uniformSampleTasks = (0 until maxNThreads).map(threadId => new Callable[Unit]{
      override def call() = {
        try{
          val localGen = new Random(uniformSampleSeeds(threadId))
          var i = threadId
          while (i < nVertices){
            u(i) = localGen.nextDouble()
            i += maxNThreads
          }
        }catch{
          case e: Exception => e.printStackTrace()
        }
      }
    }).toList
    val randThreadPool = Executors.newFixedThreadPool(maxNThreads)
    randThreadPool.invokeAll(uniformSampleTasks)
    randThreadPool.shutdown()

    for (nThreads <- 0 to maxNThreads){
      if (nThreads == 0){
        val serlMaxGraphCut = new Sequential_MaxGraphCut(nVertices, graph)

        System.gc()
        val serlStartTime_iter = System.currentTimeMillis()
        serlMaxGraphCut.run(43, u)
        val serlEndTime_iter = System.currentTimeMillis()
        val serlCutValue = MaxGraphCutFunctions.computeGraphCut(nVertices, graph, serlMaxGraphCut.A, 32)

        val serlTime = serlEndTime_iter - serlStartTime_iter

        println(s"Serial (1 threads) completed in $serlTime ms\twith objective value $serlCutValue")

      }else{

        val cc2gMaxGraphCut = new ConcurrencyControl_MaxGraphCut(nVertices, graph)
        val cf2gMaxGraphCut = new CoordinationFree_MaxGraphCut(nVertices, graph)

        val threads = Executors.newFixedThreadPool(nThreads)

        var cc2gTime : Long = 0
        var cc2gCutValue = 0.0
        if (true){
          System.gc()
          val startTime = System.currentTimeMillis()
          cc2gMaxGraphCut.run(nThreads, threads, 43, u)
          val endTime = System.currentTimeMillis()
          cc2gCutValue = MaxGraphCutFunctions.computeGraphCut(nVertices, graph, cc2gMaxGraphCut.Ahat, 32)
          cc2gTime = endTime - startTime
        }

        println(s"CC-2g  ($nThreads threads) completed in $cc2gTime ms\twith objective value $cc2gCutValue")

        var cf2gTime : Long = 0
        var cf2gCutValue = 0.0
        if (true){
          System.gc()
          val startTime = System.currentTimeMillis()
          cf2gMaxGraphCut.run(nThreads, threads, 43, u)
          val endTime = System.currentTimeMillis()
          cf2gCutValue = MaxGraphCutFunctions.computeGraphCut(nVertices, graph, cf2gMaxGraphCut.Ahat, 32)
          cf2gTime = endTime - startTime
        }

        println(s"CF-2g  ($nThreads threads) completed in $cf2gTime ms\twith objective value $cf2gCutValue")

        threads.shutdown()
      }

    }
  }
}



