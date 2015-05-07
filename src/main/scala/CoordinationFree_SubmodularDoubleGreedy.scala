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

import java.util.concurrent.{Callable, ExecutorService}
import java.util.concurrent.atomic.AtomicInteger

import collection.JavaConversions._

/**
 * Coordination-free Double Greedy (CF-2g) algorithm.
 * CF-2g runs the double greedy algorithm in parallel without any coordination.
 * The approximation factor of CF-2g is poorer than serial double greedy and CC-2g, but scales better in parallel.
 * @param nElements Number of elements in the ground set
 */
abstract class CoordinationFree_SubmodularDoubleGreedy(nElements: Int){
  /**
   * Initialization method, to be implemented by children classes.
   * May be executed in parallel
   * @param nThreads Number of threads available for parallel initialization
   * @param threads  Thread pool with nThreads threads
   * @param randSeed A random seed
   */
  protected def initialization(nThreads: Int, threads: ExecutorService, randSeed: Int) : Unit

  /**
   * Compute the marginal gains of adding / removing an element given the current sets A, B
   * @param i Element to be added / removed
   * @return  (F(A+i) - F(A), F(B-i) - F(B))
   */
  protected def computeDeltas(i: Int) : (Double, Double)

  /**
   * Update sketches and other internal data structures at the end of a transaction.
   * This method is executed after the element i is added to A or removed from B
   * @param i Element that was added to A / removed to B
   */
  protected def sketchesPostUpdate(i: Int) : Unit

  /**
   * \hat{A}; a lower bound on the true set A
   */
  val Ahat = Array.fill(nElements)(false)
  /**
   * \hat{B}: an upper bound on the true set B
   */
  val Bhat = Array.fill(nElements)(true)

  /**
   * Running the CF-2g algorithm -- including performing the initialization.
   * @param nThreads Number of threads to run CF-2g with
   * @param threads  Thread pool for running CF-2g
   * @param randSeed Random seed
   * @param u        Uniform random numbers, one for each element / transaction
   */
  def run(nThreads: Int, threads: ExecutorService, randSeed: Int, u : Array[Double]) : Unit = {
    initialization(nThreads, threads, randSeed)

    val nextToProcess = new AtomicInteger(0)
    nextToProcess.set(0)
    val tasks = (0 until nThreads).map(threadId => new Callable[Unit]{
      override def call() = {
        try{
          var haveElementsToProcess = true
          var localIndex = threadId
          while (haveElementsToProcess){
            val index = nextToProcess.getAndIncrement
            localIndex += nThreads
            if (index < nElements){
              val i = index
              // Compute deltas
              val (delta_add_, delta_rem_) = computeDeltas(i)
              val delta_add = math.max(0.0, delta_add_)
              val delta_rem = math.max(0.0, delta_rem_)
              val threshold = if (delta_add == 0.0 && delta_rem == 0.0) 1.0 else delta_add / (delta_add + delta_rem)
              if (u(i) < threshold){
                Ahat(i) = true
              }else{
                Bhat(i) = false
              }
              sketchesPostUpdate(i)
            }else{
              haveElementsToProcess = false
            }
          }
        }catch{
          case e: Exception => {
            e.printStackTrace()
          }
        }
      }
    }).toList
    threads.invokeAll(tasks)
  }



}
