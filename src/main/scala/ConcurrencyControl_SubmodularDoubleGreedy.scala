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

import scala.collection.mutable.Queue

import collection.JavaConversions._

/**
 * Concurrency Control Double Greedy (CC-2g) algorithm.
 * CC-2g runs the double greedy algorithm in parallel, coordinating between threads to ensure serializability.
 * The approximation factor of CC-2g is the same as double greedy due to serial equivalence of the outcome.
 * CC-2g does not scale as well as CF-2g because of the additional coordination required.
 * @param nElements Number of elements in the ground set
 */
abstract class ConcurrencyControl_SubmodularDoubleGreedy(nElements: Int) {
  /**
   * Compute the marginal gains of adding / removing an element given the current sets A, B
   * @param e Element to be added / removed
   * @return  (F(A+i) - F(A), F(B-i) - F(B))
   */
  protected def computeDelta(e: Int) : (Double, Double)

  /**
   * Compute the upper and lower bounds of marginal gains of adding / removing an element given the bounds on sets A, B.
   * Bounds on A, B are specified by Ahat < A < Atld, Bhat > B > Btld.
   * @param e Element to be added / removed
   * @return  (F(Atld) - F(Atld - i), F(Ahat+i) - F(Ahat), F(Btld) - F(Btld+i), F(Bhat-i) - F(Bhat))
   */
  protected def computeDeltaMinMax(e: Int) : (Double, Double, Double, Double)

  /**
   * Initialization method, to be implemented by children classes.
   * May be executed in parallel
   * @param nThreads Number of threads available for parallel initialization
   * @param threads  Thread pool with nThreads threads
   */
  protected def initialization(nThreads: Int, threads: ExecutorService) : Unit

  /**
   * Update of sketches to be executed at the start of a transaction.
   * Corresponds to getGuarantee function in paper.
   * @param e Element that was added to A / removed to B
   */
  protected def tldSketchesPreUpdate(e: Int) : Unit

  /**
   * Update sketches and other internal data structures at the end of a transaction.
   * This method is executed after the element i is added to A or removed from B
   * @param e Element that was added to A / removed to B
   */
  protected def sketchesPostUpdate(e: Int) : Unit

  /**
   * Lower bound on A
   */
  val Ahat = Array.fill(nElements)(false)

  /**
   * Upper bound on A
   */
  val Atld = Array.fill(nElements)(false)

  /**
   * Upper bound on B
   */
  val Bhat = Array.fill(nElements)(true)

  /**
   * Lower bound on B
   */
  val Btld = Array.fill(nElements)(true)

  /**
   * Array of results, indicating whether to include / exclude an element.
   * result(i) is set when we have made a decision on the i-th element
   */
  protected val result = Array.fill(nElements)(0)

  /**
   * Array indicating whether a transaction has completed.
   * After a decision has been made on the i-th element, we do not immediately update Ahat/Atld/Bhat/Btld, because the sets are required for computing other decisions.
   */
  protected val committed = Array.fill(nElements)(0)

  /**
   * The largest i for which result(i) is non-zero.
   * This is maintained by each thread locally, so as to avoid coordination and prevent conflicts.
   */
  protected var maxResult = new Array[Int](0)

  /**
   * The largest i for which committed(i) is non-zero.
   * This is maintained by each thread locally, so as to avoid coordination and prevent conflicts.
   */
  protected var maxCommitted = new Array[Int](0)

  /**
   * Queue on each thread with elements for which results are decided but not committed, i.e. not updated to A/B.
   * Elements in queue are repeatedly committed when possible.
   */
  protected var commitQueue = new Array[Queue[(Int,Int)]](0)

  /**
   * Number of validations of CC-2g
   */
  val numValidations = new AtomicInteger(0)

  /**
   * Effective ordering that resulted from processing CC-2g.
   * That is, the serial ordering for which CC-2g generated an equivalent result.
   */
  val ordering = Array.fill(nElements)(0)

  /**
   * Commits elements in queue, i.e. transactions in-flight.
   * Corresponds to the commit function in the paper.
   * @param p Queue to commit, corresponding to the p-th processor.
   */
  protected def commitElementInQueue(p: Int) = {
    var hasElementInQueueToCommit = true
    while (hasElementInQueueToCommit){
      while (maxResult(p)<nElements && result(maxResult(p)) != 0){
        maxResult(p) += 1
      }
      if (commitQueue(p).isEmpty){
        hasElementInQueueToCommit = false
      }else if (commitQueue(p).head._1 > maxResult(p)){
        hasElementInQueueToCommit = false
      }else{
        val (ii, ee) = commitQueue(p).dequeue
        if (result(ii) == 1){
          Ahat(ee) = true
          Btld(ee) = true
        }else{
          Atld(ee) = false
          Bhat(ee) = false
        }
        sketchesPostUpdate(ee)
        committed(ii) = 1
      }
    }
  }

  /**
   * Running the CC-2g algorithm -- including performing the initialization.
   * @param nThreads Number of threads to run CC-2g with
   * @param threads  Thread pool for running CC-2g
   * @param randSeed Random seed
   * @param u        Uniform random numbers, one for each element / transaction
   */
  def run(nThreads: Int, threads: ExecutorService, randSeed: Int, u : Array[Double]) : Unit = {
    val nextToProcess = new AtomicInteger(0)
    val iota = new AtomicInteger(0)
    initialization(nThreads, threads)
    nextToProcess.set(0)
    iota.set(0)
    maxResult = Array.fill(nThreads)(0)
    maxCommitted = Array.fill(nThreads)(0)
    commitQueue = Array.tabulate(nThreads)(_ => new Queue[(Int, Int)]())
    val tasks = (0 until nThreads).map(p => new Callable[Unit]{
      override def call() = {
        try{
          var haveElementsToProcess = true
          var localIndex = p
          while (haveElementsToProcess){
            val index = nextToProcess.getAndIncrement
            localIndex += nThreads
            if (index < nElements){
              val e = index
              // Adjust Atld, Btld
              Atld(e) = true
              Btld(e) = false
              tldSketchesPreUpdate(e)
              // Get serialization order
              val i = iota.getAndIncrement
              ordering(i) = e
              var isValidated = false
              while (result(i) == 0){
                commitElementInQueue(p)
                val (delta_addmin_, delta_addmax_, delta_remmin_, delta_remmax_) = computeDeltaMinMax(e)
                val (lb, ub) = getThresholdUpperLowerBounds(delta_addmin_, delta_addmax_, delta_remmin_, delta_remmax_)
                // Try to assign
                if (u(e) < lb){
                  result(i) = 1
                }else if (u(e) >= ub){
                  result(i) = -1
                }
                if (result(i) == 0){
                  // Check if can validate
                  while (maxCommitted(p) < i && committed(maxCommitted(p)) == 1){
                    maxCommitted(p) += 1
                  }
                  if (maxCommitted(p) == i){
                    // Compute deltas
                    val (delta_add, delta_rem) = computeDelta(e)
                    val threshold = getThreshold(delta_add, delta_rem)
                    if (u(e) < threshold){
                      result(i) = 1
                    }else{
                      result(i) = -1
                    }
                  }
                  isValidated = true
                }
              }
              if (isValidated) numValidations.getAndIncrement
              commitQueue(p).enqueue((i,e))
            }else{
              haveElementsToProcess = false
            }
          }
          while (!commitQueue(p).isEmpty) commitElementInQueue(p)
        }catch{
          case err: Exception => {
            err.printStackTrace()
          }
        }
      }
    }).toList
    threads.invokeAll(tasks)
  }

  /**
   * Compute the upper and lower bounds of the probability threshold given bounds on marginal gains.
   * @param delta_addmin_ F(Atld  ) - F(Atld - i)
   * @param delta_addmax_ F(Ahat+i) - F(Ahat)
   * @param delta_remmin_ F(Btld  ) - F(Btld+i)
   * @param delta_remmax_ F(Bhat-i) - F(Bhat)
   * @return (lower bound, upper bound) of probability threshold
   */
  protected def getThresholdUpperLowerBounds(delta_addmin_ : Double, delta_addmax_ : Double, delta_remmin_ : Double, delta_remmax_ : Double) : (Double, Double) = {
    val delta_addmin = math.max(0.0, delta_addmin_)
    val delta_addmax = math.max(0.0, delta_addmax_)
    val delta_remmin = math.max(0.0, delta_remmin_)
    val delta_remmax = math.max(0.0, delta_remmax_)
    // Determine upper and lower thresholds
    var lb = delta_addmin / (delta_addmin + delta_remmax)
    var ub = delta_addmax / (delta_addmax + delta_remmin)
    if (delta_remmax == 0.0){
      if (delta_addmin == 0.0){
        // if delta_addmax = 0, then both deltas are upper bounded by 0, so our choice can be arbitrary
        // if delta_addmax > 0, then we should prefer to include the element
        lb = 1.0
        ub = 1.0
      }
    }else if (delta_addmax == 0.0){
      if (delta_remmin == 0.0){
        // if delta_remmax = 0, then both deltas are lower bounded by 0, so our choice can be arbitrary
        // if delta_remmax > 0, then we should prefer to exclude the element
        lb = 0.0
        ub = 0.0
      }
    }
    (lb, ub)
  }

  /**
   * Compute the probability threshold given the true marginal gains.
   * @param delta_add_ F(Atld  ) - F(Atld - i)
   * @param delta_rem_ F(Btld  ) - F(Btld+i)
   * @return probability threshold
   */
  protected def getThreshold(delta_add_ : Double, delta_rem_ : Double) : Double = {
    val delta_add = math.max(0.0, delta_add_)
    val delta_rem = math.max(0.0, delta_rem_)
    val threshold = if (delta_add == 0.0 && delta_rem == 0.0) 1.0 else delta_add / (delta_add + delta_rem)
    threshold
  }

}

