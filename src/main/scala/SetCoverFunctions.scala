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

import java.util.concurrent.{Callable, Executors}
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.util.concurrent.{AtomicDouble, AtomicDoubleArray}

import collection.JavaConversions._

/**
 * Static object for functions related to set cover, specifically for computing the objective value.
 */
object SetCoverFunctions{

  /**
   * Computes the set cover objective value, given a graph and the output of the double greedy algorithm
   * @param nElements Number of elements in graph
   * @param nGroups   Number of groups / covers
   * @param A         Output of double greedy algorithm
   * @param lambda    Weight of cardinality term in objective function
   * @param graph     Graph on which set cover is evaluated
   * @param nThreads  Number of threads to use to compute objective value
   * @return          Objective value, i.e. the set cover value
   */
  def computeSetCover(nElements: Int, nGroups: Int, A: Array[Boolean], lambda: Double, graph: SparseGraph, nThreads: Int) : Double = {
    val alpha = new AtomicDoubleArray(nGroups)
    var i = 0
    while (i < nElements){
      if (A(i)){
        var k = 0
        val succLen = graph.nSuccessor(i)
        while (k < succLen){
          val ll = graph.succ(i, k)
          alpha.getAndAdd(ll, 1.0)
          k += 1
        }
      }
      i += 1
    }
    computeSetCover(nElements, nGroups, A, lambda, alpha, nThreads)
  }

  /**
   * Computes the set cover objective value, given the output of the double greedy algorithm
   * @param nElements Number of elements in graph
   * @param nGroups   Number of groups / covers
   * @param A         Output of double greedy algorithm
   * @param lambda    Weight of cardinality term in objective function
   * @param alpha     Sketch alpha: sum of weights of elements in A
   * @param nThreads  Number of threads to use to compute objective value
   * @return          Objective value, i.e. the set cover value
   */
  def computeSetCover(nElements: Int, nGroups: Int, A: Array[Boolean], lambda: Double, alpha: AtomicDoubleArray, nThreads: Int) : Double = {

    val threads = Executors.newFixedThreadPool(nThreads)

    val coverVal = new AtomicDouble(0.0)

    val indexElement = new AtomicInteger(0)
    val indexGroup   = new AtomicInteger(0)

    val tasks = (0 until nThreads).map(threadId => new Callable[Unit]{
      override def call() = {
        try{
          var hasElementToProcess = true
          while (hasElementToProcess){
            val i = indexElement.getAndIncrement
            if (i < nElements){
              if (A(i)) coverVal.getAndAdd(-lambda)
            }else{
              hasElementToProcess = false
            }
          }
          var hasGroupToProcess = true
          while (hasGroupToProcess){
            val i = indexGroup.getAndIncrement
            if (i < nGroups){
              if (alpha.get(i) >= 1.0) coverVal.getAndAdd(1.0)
            }else{
              hasGroupToProcess = false
            }
          }
        }catch{
          case e: Exception => e.printStackTrace()
        }
      }
    }).toList
    threads.invokeAll(tasks)

    threads.shutdown()

    coverVal.get()

  }

}