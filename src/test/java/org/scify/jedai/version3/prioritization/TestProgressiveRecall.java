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
package org.scify.jedai.prioritization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.blockbuilding.IBlockBuilding;
import org.scify.jedai.blockbuilding.StandardBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.blockcleaning.ComparisonsBasedBlockPurging;
import org.scify.jedai.blockprocessing.comparisoncleaning.CardinalityNodePruning;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.datamodel.ComparisonIterator;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

/**
 *
 * @author gap2
 */
public class TestProgressiveRecall {

    private final static int NO_OF_ITERATIONS = 10;

    public static void main(String[] args) {
        BasicConfigurator.configure();

        String mainDir = "C:\\Users\\Georgios\\Documents\\Data\\ER\\ccer\\top10\\";
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "imdbProfilesNEW", "imdbProfilesNEW", "tmdbProfiles", "walmartProfiles", "dblpProfiles2", "imdbProfiles"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "tmdbProfiles", "tvdbProfiles", "tvdbProfiles", "amazonProfiles2", "scholarProfiles", "dbpediaProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "imdbTmdbIdDuplicates", "imdbTvdbIdDuplicates", "tmdbTvdbIdDuplicates", "amazonWalmartIdDuplicates",
            "dblpScholarIdDuplicates", "moviesIdDuplicates"};

        for (int i = 0; i < groundtruthDirs.length; i++) {
            final IEntityReader eReader1 = new EntitySerializationReader(mainDir + datasetsD1[i]);
            final List<EntityProfile> profiles1 = eReader1.getEntityProfiles();
            System.out.println("\n\n\n\n\nInput Entity Profiles\t:\t" + profiles1.size());

            final IEntityReader eReader2 = new EntitySerializationReader(mainDir + datasetsD2[i]);
            final List<EntityProfile> profiles2 = eReader2.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles2.size());

            final IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthDirs[i]);
            final Set<IdDuplicates> duplicatePairs = gtReader.getDuplicatePairs(null);
            AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(duplicatePairs);
            System.out.println("Existing Duplicates\t:\t" + duplicatePairs.size());

            float time1 = System.currentTimeMillis();

            final IBlockBuilding blockBuildingMethod = new StandardBlocking();
            List<AbstractBlock> blocks = blockBuildingMethod.getBlocks(profiles1, profiles2);
            System.out.println("Original blocks\t:\t" + blocks.size());

            final IBlockProcessing blockCleaningMethod1 = new ComparisonsBasedBlockPurging(1.00f);
            blocks = blockCleaningMethod1.refineBlocks(blocks);

            final IBlockProcessing blockCleaningMethod2 = new BlockFiltering();
            blocks = blockCleaningMethod2.refineBlocks(blocks);

            final IBlockProcessing comparisonCleaningMethod = new CardinalityNodePruning(WeightingScheme.JS);
            List<AbstractBlock> cnpBlocks = comparisonCleaningMethod.refineBlocks(new ArrayList<>(blocks));

            float time2 = System.currentTimeMillis();

            BlocksPerformance blStats = new BlocksPerformance(cnpBlocks, duplicatePropagation);
            blStats.setStatistics();
            blStats.printStatistics(time2 - time1, "", "");

            double totalComparisons = blStats.getAggregateCardinality();
            System.out.println("Total comparisons\t:\t" + totalComparisons);

//            final IPrioritization prioritization = new ProgressiveBlockScheduling((int) totalComparisons, WeightingScheme.ARCS);
//            final IPrioritization prioritization = new ProgressiveEntityScheduling((int) totalComparisons, WeightingScheme.ARCS);
//            final IPrioritization prioritization = new ProgressiveLocalTopComparisons((int) totalComparisons, WeightingScheme.ARCS);
            final IPrioritization prioritization = new ProgressiveGlobalTopComparisons((int) totalComparisons, WeightingScheme.JS);
//            final IPrioritization prioritization = new ProgressiveGlobalRandomComparisons((int) totalComparisons);
            prioritization.developBlockBasedSchedule(cnpBlocks);

//            final IPrioritization prioritization = new LocalProgressiveSortedNeighborhood(profiles1.size() * profiles2.size(), ProgressiveWeightingScheme.ACF);
//            final IPrioritization prioritization = new GlobalProgressiveSortedNeighborhood(profiles1.size() * profiles2.size(), ProgressiveWeightingScheme.ACF);
//            prioritization.developEntityBasedSchedule(profiles1, profiles2);
            int counter = 0;
            int detectedMatches = 0;
            double progressiveRecall = 0;
            while (prioritization.hasNext()) {
                counter++;
                Comparison c = prioritization.next();
                if (duplicatePairs.contains(new IdDuplicates(c.getEntityId1(), c.getEntityId2()))) {
                    detectedMatches++;
                }
                progressiveRecall += detectedMatches;
            }

            int existingDuplicates = blStats.getDetectedDuplicates();
            double auc = progressiveRecall / existingDuplicates / (counter + 1.0);
            System.out.println("AUC (Progressive Recall)\t:\t" + auc);

            //Baseline method
            List<Comparison> allComparisons = new ArrayList<>();
            for (AbstractBlock block : cnpBlocks) {
                final ComparisonIterator cIterator = block.getComparisonIterator();
                while (cIterator.hasNext()) {
                    allComparisons.add(cIterator.next());
                }
            }
            System.out.println("Total comparisons\t:\t" + allComparisons.size());

            double averageAUC = 0;
            for (int iteration = 0; iteration < NO_OF_ITERATIONS; iteration++) {
                Collections.shuffle(allComparisons);

                counter = 0;
                detectedMatches = 0;
                progressiveRecall = 0;
                for (Comparison c : allComparisons) {
                    counter++;
                    if (duplicatePairs.contains(new IdDuplicates(c.getEntityId1(), c.getEntityId2()))) {
                        detectedMatches++;
                    }
                    progressiveRecall += detectedMatches;
                }
                averageAUC += progressiveRecall / existingDuplicates / (counter + 1.0);
            }
            System.out.println("Baseline AUC (Progressive Recall)\t:\t" + averageAUC / NO_OF_ITERATIONS);
        }
    }
}
