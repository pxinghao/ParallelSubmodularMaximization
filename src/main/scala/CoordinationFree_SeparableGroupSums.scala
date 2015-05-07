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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, ExecutorService}

import com.google.common.util.concurrent.AtomicDoubleArray

import collection.JavaConversions._

/**
 * Class implementing CF-2g for separable sums objective functions, of the form F(X) = \sum_l g(\sum_{i\in S_l\cap X} w_l(i)) - \lambda|X|.
 * See Appendix F of paper for more details.
 * @param nElements Number of elements in the ground set
 * @param nGroups   Number of groups, i.e. number of summands in objective function
 * @param graph     A graph in SparseGraph format indicating membership of elements in group, i.e. an edge (i,S) exists in graph if i is an element of group S
 * @param lambda    Weight of cardinality term in objective function
 */
abstract class CoordinationFree_SeparableGroupSums(nElements: Int, nGroups: Int, graph: SparseGraph, lambda: Double)
  extends CoordinationFree_SubmodularDoubleGreedy(nElements){

  /**
   * Implements the scalar function g (non-decreasing, concave) of the objective function
   * @param sumW Sum of weights \sum_{i\in S_l\cap X} w_l(i) of elements
   * @return     Result of applying g to the input value
   */
  protected def g(sumW: Double) : Double

  /**
   * \hat{\alpha} sketch: sum of weights of elements in \hat{A}
   */
  val alphhat : AtomicDoubleArray = new AtomicDoubleArray(nGroups)

  /**
   * \hat{\beta} sketch: sum of weights of elements in \hat{B}
   */
  val betahat : AtomicDoubleArray = new AtomicDoubleArray(nGroups)

  /**
   * Initialization function for separable group sums.
   * Sets betahat for every group.
   * @param nThreads Number of threads available for parallel initialization
   * @param threads  Thread pool with nThreads threads
   * @param randSeed A random seed
   */
  protected def initialization(nThreads: Int, threads: ExecutorService, randSeed: Int) : Unit = {
    val nextToProcess = new AtomicInteger(0)
    nextToProcess.set(0)
    // Initialization
    val initTasks = (0 until nThreads).map(threadId => new Callable[Unit]{
      override def call() = {
        try{
          var haveElementsToProcess = true
          while (haveElementsToProcess){
            val i = nextToProcess.getAndIncrement
            if (i < nElements){
              var k = 0
              val succLen = graph.nSuccessor(i)
              while (k < succLen){
                val ll = graph.succ(i, k)
                betahat.getAndAdd(ll, 1.0)
                k += 1
              }
            }else{
              haveElementsToProcess = false
            }
          }
        }catch{
          case e: Exception => e.printStackTrace()
        }
      }
    }).toList

    threads.invokeAll(initTasks)
  }

  /**
   * Implements the computation of marginal gains for separable group sums.
   * @param i Element to be added / removed
   * @return  (F(A+i) - F(A), F(B-i) - F(B))
   */
  protected def computeDeltas(i: Int) : (Double, Double) = {
    var delta_add = -lambda
    var delta_rem =  lambda
    var k = 0
    val succLen = graph.nSuccessor(i)
    while (k < succLen){
      val ll = graph.succ(i, k)
      val alph = alphhat.get(ll)
      val beta = betahat.get(ll)
      delta_add += g(alph + 1.0) - g(alph)
      delta_rem += g(beta - 1.0) - g(beta)
      k += 1
    }
    (delta_add, delta_rem)
  }

  /**
   * Updates the sketches of alphhat and betahat at the end of transaction i
   * @param i Element that was added to A / removed to B
   */
  protected def sketchesPostUpdate(i: Int) : Unit = {
    if (Ahat(i)){
      var k = 0
      val succLen = graph.nSuccessor(i)
      while (k < succLen){
        val ll = graph.succ(i, k)
        alphhat.getAndAdd(ll, 1.0)
        k += 1
      }
    }else{
      var k = 0
      val succLen = graph.nSuccessor(i)
      while (k < succLen){
        val ll = graph.succ(i, k)
        betahat.getAndAdd(ll, -1.0)
        k += 1
      }
    }
  }


}


