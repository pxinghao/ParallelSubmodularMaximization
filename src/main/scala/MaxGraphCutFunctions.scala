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

import com.google.common.util.concurrent.AtomicDouble

import collection.JavaConversions._

/**
 * Static object for functions related to max graph cut, specifically for computing the objective value.
 */
object MaxGraphCutFunctions{

  /**
   * Computes the max graph cut objective value, given a graph and the output of the double greedy algorithm
   * @param nElements Number of elements in graph
   * @param graph     Graph on which max graph cut is evaluated
   * @param A         Output of double greedy algorithm
   * @param nThreads  Number of threads to use to compute objective value
   * @return          Objective value, i.e. the graph cut value
   */
  def computeGraphCut(nElements: Int, graph: SparseGraph, A: Array[Boolean], nThreads: Int) : Double = {
    val cutVal = new AtomicDouble(0)

    val threads = Executors.newFixedThreadPool(nThreads)

    val index = new AtomicInteger(0)

    val tasks = (0 until nThreads).map(threadId => new Callable[Unit] {
      override def call() = {
        try{
          var hasElementToProcess = true
          while (hasElementToProcess){
            val i = index.getAndIncrement
            if (i < nElements){
              var thisCutVal = 0.0
              var k = 0
              val succLen = graph.nSuccessor(i)
              while (k < succLen){
                val j = graph.succ(i, k)
                if ((A(i) && !A(j)) || (!A(i) && A(j))){
                  thisCutVal += 1.0
                }
                k += 1
              }
              cutVal.getAndAdd(thisCutVal)
            }else{
              hasElementToProcess = false
            }
          }
        }catch{
          case e: Exception => e.printStackTrace()
        }
      }
    }).toList
    threads.invokeAll(tasks)

    threads.shutdown()

    cutVal.get()

  }

}
