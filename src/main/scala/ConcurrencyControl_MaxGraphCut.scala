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

import java.util.concurrent.ExecutorService

/**
 * Class implementing CC-2g max graph cut
 * @param nElements Number of elements in the ground set
 * @param graph     Graph on which to perform graph cut
 */
class ConcurrencyControl_MaxGraphCut(nElements: Int, val graph: SparseGraph)
  extends ConcurrencyControl_SubmodularDoubleGreedy(nElements) {

  /**
   * Compute the marginal gains of adding / removing an element given the current sets A, B
   * @param e Element to be added / removed
   * @return  (F(A+i) - F(A), F(B-i) - F(B))
   */
  protected def computeDelta(e: Int) : (Double, Double) = {
    var delta_add_ = 0.0
    var delta_rem_ = 0.0
    var k = 0
    val succLen = graph.nSuccessor(e)
    while (k < succLen){
      val j = graph.succ(e, k)
      delta_add_ += (if (!Ahat(j)) 1.0 else -1.0)
      delta_rem_ += (if ( Bhat(j)) 1.0 else -1.0)
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
    // Compute deltas
    var delta_addmin_ = 0.0
    var delta_addmax_ = 0.0
    var delta_remmin_ = 0.0
    var delta_remmax_ = 0.0
    var k = 0
    val succLen = graph.nSuccessor(e)
    while (k < succLen){
      val j = graph.succ(e, k)
      delta_addmin_ += (if (!Atld(j)) 1.0 else -1.0)
      delta_addmax_ += (if (!Ahat(j)) 1.0 else -1.0)
      delta_remmin_ += (if ( Btld(j)) 1.0 else -1.0)
      delta_remmax_ += (if ( Bhat(j)) 1.0 else -1.0)
      k += 1
    }
    (delta_addmin_, delta_addmax_, delta_remmin_, delta_remmax_)
  }

  /**
   * Initialization for CC-2g max graph cut.
   * Nothing needs to be initialized.
   * @param nThreads Number of threads available for parallel initialization
   * @param threads  Thread pool with nThreads threads
   */
  protected def initialization(nThreads: Int, threads: ExecutorService) : Unit = {}

  /**
   * Update of sketches to be executed at the start of a transaction.
   * Nothing needs to be updated at the start of a transaction.
   * @param e Element that was added to A / removed to B
   */
  protected def tldSketchesPreUpdate(e: Int) : Unit = {}

  /**
   * Update sketches and other internal data structures at the end of a transaction.
   * Nothing needs to be updated at the end of a transaction.
   * @param e Element that was added to A / removed to B
   */
  protected def sketchesPostUpdate(e: Int) : Unit = {}
}






