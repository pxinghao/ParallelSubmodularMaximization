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

import com.google.common.util.concurrent.AtomicDoubleArray

import collection.JavaConversions._

/**
 * Class implementing CC-2g for separable sums objective functions, of the form F(X) = \sum_l g(\sum_{i\in S_l\cap X} w_l(i)) - \lambda|X|.
 * See Appendix F of paper for more details.
 * @param nElements Number of elements in the ground set
 * @param nGroups   Number of groups, i.e. number of summands in objective function
 * @param graph     A graph in SparseGraph format indicating membership of elements in group, i.e. an edge (i,S) exists in graph if i is an element of group S
 * @param lambda    Weight of cardinality term in objective function
 */
abstract class ConcurrencyControl_SeparableGroupSums(nElements: Int, nGroups: Int, graph: SparseGraph, lambda: Double)
  extends ConcurrencyControl_SubmodularDoubleGreedy(nElements) {

  /**
   * Implements the scalar function g (non-decreasing, concave) of the objective function
   * @param sumW Sum of weights \sum_{i\in S_l\cap X} w_l(i) of elements
   * @return     Result of applying g to the input value
   */
  protected def g(sumW: Double) : Double

  /**
   * \hat{\alpha} sketch: sum of weights of elements in \hat{A}
   */
  val alphhat = new AtomicDoubleArray(nGroups)

  /**
   * \tilde{\alpha} sketch: sum of weights of elements in \tilde{A}
   */
  val alphtld = new AtomicDoubleArray(nGroups)

  /**
   * \hat{\beta} sketch: sum of weights of elements in \hat{B}
   */
  val betahat = new AtomicDoubleArray(nGroups)

  /**
   * \tilde{\beta} sketch: sum of weights of elements in \tilde{B}
   */
  val betatld = new AtomicDoubleArray(nGroups)

  /**
   * Implements the computation of marginal gains for separable group sums.
   * @param e Element to be added / removed
   * @return  (F(A+i) - F(A), F(B-i) - F(B))
   */
  protected def computeDelta(e: Int) : (Double, Double) = {
    val lambdav = lambda //* v(e)
    var delta_add_ = -lambdav
    var delta_rem_ = +lambdav
    var k = 0
    val succLen = graph.nSuccessor(e)
    while (k < succLen){
      val ll = graph.succ(e, k)
      val alph = alphhat.get(ll)
      val beta = betahat.get(ll)
      delta_add_ += g(alph + 1.0) - g(alph)
      delta_rem_ += g(beta - 1.0) - g(beta)
      k += 1
    }
    (delta_add_, delta_rem_)
  }

  /**
   * Compute the upper and lower bounds of marginal gains of adding / removing an element given the bounds on sets A, B.
   * Bounds on A, B are specified by Ahat < A < Atld, Bhat > B > Btld.
   * @param e Element to be added / removed
   * @return  (F(Atld) - F(Atld - i), F(Ahat+i) - F(Ahat), F(Btld) - F(Btld+i), F(Bhat-i) - F(Bhat))
   */
  protected def computeDeltaMinMax(e: Int) : (Double, Double, Double, Double) = {
    val lambdav = lambda //* v(e)
    var delta_addmin_ = -lambdav
    var delta_addmax_ = -lambdav
    var delta_remmin_ = +lambdav
    var delta_remmax_ = +lambdav
    var k = 0
    val succLen = graph.nSuccessor(e)
    while (k < succLen){
      val ll = graph.succ(e, k)
      val alph4min = alphtld.get(ll)
      val alph4max = alphhat.get(ll)
      val beta4min = betatld.get(ll)
      val beta4max = betahat.get(ll)
      delta_addmin_ += g(alph4min      ) - g(alph4min - 1.0)
      delta_addmax_ += g(alph4max + 1.0) - g(alph4max    )
      delta_remmin_ += g(beta4min      ) - g(beta4min + 1.0)
      delta_remmax_ += g(beta4max - 1.0) - g(beta4max    )
      k += 1
    }
    (delta_addmin_, delta_addmax_, delta_remmin_, delta_remmax_)
  }

  /**
   * Initialization function for separable group sums.
   * Sets betahat and betatld for every group.
   * @param nThreads Number of threads available for parallel initialization
   * @param threads  Thread pool with nThreads threads
   */
  protected def initialization(nThreads: Int, threads: ExecutorService) : Unit = {
    val index = new AtomicInteger(0)
    val initTasks = (0 until nThreads).map(threadId => new Callable[Unit]{
      override def call() = {
        try{
          var haveElementsToProcess = true
          while (haveElementsToProcess){
            val e = index.getAndIncrement
            if (e < nElements){
              var k = 0
              val succLen = graph.nSuccessor(e)
              while (k < succLen){
                val ll = graph.succ(e, k)
                betahat.getAndAdd(ll, 1.0)
                betatld.getAndAdd(ll, 1.0)
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
   * Update of sketches to be executed at the start of a transaction.
   * Corresponds to getGuarantee function in paper.
   * @param e Element that was added to A / removed to B
   */
  protected def tldSketchesPreUpdate(e: Int) : Unit = {
    var k = 0
    val succLen = graph.nSuccessor(e)
    while (k < succLen){
      val ll = graph.succ(e, k)
      alphtld.getAndAdd(ll, +1.0)
      betatld.getAndAdd(ll, -1.0)
      k += 1
    }
  }

  /**
   * Update sketches and other internal data structures at the end of a transaction.
   * This method is executed after the element i is added to A or removed from B
   * @param e Element that was added to A / removed to B
   */
  protected def sketchesPostUpdate(e: Int) : Unit = {
    if (Ahat(e)){
      var k = 0
      val succLen = graph.nSuccessor(e)
      while (k < succLen){
        val ll = graph.succ(e, k)
        alphhat.getAndAdd(ll, +1.0)
        betatld.getAndAdd(ll, +1.0)
        k += 1
      }
    }else{
      var k = 0
      val succLen = graph.nSuccessor(e)
      while (k < succLen){
        val ll = graph.succ(e, k)
        alphtld.getAndAdd(ll, -1.0)
        betahat.getAndAdd(ll, -1.0)
        k += 1
      }
    }
  }
}



