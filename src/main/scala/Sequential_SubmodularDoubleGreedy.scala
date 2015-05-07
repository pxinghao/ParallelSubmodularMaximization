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

/**
 * Sequential Double Greedy algorithm for maximizing non-monotone unconstrained submodular functions.
 * @param nElements Number of elements in the ground set
 */
abstract class Sequential_SubmodularDoubleGreedy(nElements: Int){
  /**
   * Initialization method, to be implemented by children classes.
   * @param randSeed A random seed
   */
  protected def initialization(randSeed: Int) : Unit

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
   * An increasing set initialized to empty, with elements added as algorithm proceeds.
   */
  val A = Array.fill(nElements)(false)

  /**
   * An decreasing set initialized to full ground set, with elements removed as algorithm proceeds.
   */
  val B = Array.fill(nElements)(true)

  /**
   * Running the sequential double greedy algorithm -- including performing the initialization.
   * @param randSeed Random seed
   * @param u        Uniform random numbers, one for each element / transaction
   */
  def run(randSeed: Int, u : Array[Double]) = {
    initialization(randSeed)

    val nextToProcess = new AtomicInteger(0)
    nextToProcess.set(0)
    var haveElementsToProcess = true
    var localIndex = 0
    while (haveElementsToProcess){
      val index = nextToProcess.getAndIncrement
      localIndex += 1
      if (index < nElements){
        val i = index
        // Compute deltas
        val (delta_add_, delta_rem_) = computeDeltas(i)
        val delta_add = math.max(0.0, delta_add_)
        val delta_rem = math.max(0.0, delta_rem_)
        val threshold = if (delta_add == 0.0 && delta_rem == 0.0) 1.0 else delta_add / (delta_add + delta_rem)
        if (u(i) < threshold){
          A(i) = true
        }else{
          B(i) = false
        }
        sketchesPostUpdate(i)
      }else{
        haveElementsToProcess = false
      }
    }
  }

}
