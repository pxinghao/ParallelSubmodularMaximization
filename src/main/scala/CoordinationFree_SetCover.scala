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
 * Class implementing CF-2g set cover as a separable group sum objective function.
 * @param nElements Number of elements in the ground set
 * @param nGroups   Number of groups, i.e. number of summands in objective function
 * @param graph     A graph in SparseGraph format indicating membership of elements in group, i.e. an edge (i,S) exists in graph if i is an element of group S
 * @param lambda    Weight of cardinality term in objective function
 */
class CoordinationFree_SetCover(nElements: Int, nGroups: Int, graph: SparseGraph, lambda: Double)
  extends CoordinationFree_SeparableGroupSums(nElements, nGroups, graph, lambda){
  protected def g(sumW: Double) : Double = {
    math.min(1.0, sumW)
  }
}


