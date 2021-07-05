/*
* Copyright [2016-2020] [George Papadakis (gpapadis@yahoo.gr)]
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package org.scify.jedai.utilities.enumerations;

import org.scify.jedai.entityclustering.*;

/**
 *
 * @author GAP2
 */
public enum EntityClusteringCcerMethod {
	UNIQUE_MAPPING_CLUSTERING,
	ROW_COLUMN_ASSIGNMENT_CLUSTERING,
    BEST_ASSIGNMENT_HEURISTIC_CLUSTERING,
    BEST_ASSIGNMENT_HEURISTIC_CLUSTERINGTO,
    CENTER_CLUSTERING_CCER,
    CENTER_CLUSTERING_CCERsingleEdge,
    MARKOV_CLUSTERING_CCER,
    RICOCHETSR_CLUSTERING_CCER,
    RICOCHETSR_CLUSTERING_CCERsingleEdge;

    public static IEntityClustering getDefaultConfiguration(EntityClusteringCcerMethod ecMethod) {
        switch (ecMethod) {
        case UNIQUE_MAPPING_CLUSTERING:
            return new UniqueMappingClustering();
        case ROW_COLUMN_ASSIGNMENT_CLUSTERING:
            return new RowColumnClustering();
        case BEST_ASSIGNMENT_HEURISTIC_CLUSTERING:
            return new BestAssignmentHeuristic();
        case BEST_ASSIGNMENT_HEURISTIC_CLUSTERINGTO:
            return new BestAssignmentHeuristicWithTO();
        case CENTER_CLUSTERING_CCER:
            return new CenterClusteringCCER();
        case MARKOV_CLUSTERING_CCER:
            return new MarkovClusteringCCER();
        case RICOCHETSR_CLUSTERING_CCER:
            return new RicochetSRClusteringCCER();
        case RICOCHETSR_CLUSTERING_CCERsingleEdge:
            return new RicochetSRClusteringCCERsingleEdge();
        case CENTER_CLUSTERING_CCERsingleEdge:
            return new CenterClusteringCCERsingleEdge();
        default:
            return new UniqueMappingClustering();
        }
    }
}
