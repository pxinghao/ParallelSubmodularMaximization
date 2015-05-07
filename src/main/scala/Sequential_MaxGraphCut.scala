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

/**
 * Class implementing double greedy max graph cut
 * @param nVertices
 * @param graph
 */
class Sequential_MaxGraphCut(val nVertices: Int, val graph: SparseGraph)
  extends Sequential_SubmodularDoubleGreedy(nVertices){
  /**
   * Initialization for max graph cut.
   * Nothing needs to be initialized.
   * @param randSeed A random seed
   */
  protected def initialization(randSeed: Int) : Unit = {}

  /**
   * Compute the marginal gains of adding / removing an element given the current sets A, B
   * @param i Element to be added / removed
   * @return  (F(A+i) - F(A), F(B-i) - F(B))
   */
  protected def computeDeltas(i: Int) : (Double, Double) = {
    var delta_add = 0.0
    var delta_rem = 0.0
    var k = 0
    val succLen = graph.nSuccessor(i)
    while (k < succLen){
      val j = graph.succ(i, k)
      delta_add += (if (!A(j)) 1.0 else -1.0)
      delta_rem += (if ( B(j)) 1.0 else -1.0)
      k += 1
    }
    (delta_add, delta_rem)
  }

  /**
   * Update sketches and other internal data structures at the end of a transaction.
   * There are no updates for max graph cut.
   * @param i Element that was added to A / removed to B
   */
  protected def sketchesPostUpdate(i: Int) : Unit = {}

}
