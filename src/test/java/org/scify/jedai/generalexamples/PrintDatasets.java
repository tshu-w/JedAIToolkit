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
package org.scify.jedai.generalexamples;

import java.io.File;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.datamodel.Attribute;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;

import java.util.List;
import java.util.Set;
import org.scify.jedai.datamodel.IdDuplicates;

/**
 *
 * @author G.A.P. II
 */
public class PrintDatasets {

    private static void printDataset(String filePath) {
        final IEntityReader eReader = new EntitySerializationReader(filePath);
        List<EntityProfile> profiles = eReader.getEntityProfiles();
        System.out.println("\n\n\n\n\nDataset\t:\t" + filePath);
        System.out.println("Number of Entity Profiles\t:\t" + profiles.size());

        for (EntityProfile profile : profiles) {
            System.out.println("\nProfile id\t:\t" + profile.getEntityUrl());
            for (Attribute attribute : profile.getAttributes()) {
                System.out.println(attribute.getName() + " : " + attribute.getValue());
            }
        }
    }

    private static void printGroundtruth(String filePath) {
        IGroundTruthReader gtReader = new GtSerializationReader(filePath);
        Set<IdDuplicates> duplicates = gtReader.getDuplicatePairs(null);
        System.out.println("\n\n\n\n\nDataset groundtruth\t:\t" + filePath);
        for (IdDuplicates pair : duplicates) {
            System.out.println(pair.getEntityId1() + " = " + pair.getEntityId2());
        }
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();

        String[] entitiesFilePath = {"data" + File.separator + "cleanCleanErDatasets" + File.separator + "abtProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "buyProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "amazonProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "gpProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "dblpProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "acmProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "dblpProfiles2",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "scholarProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "imdbProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "dbpediaProfiles"
        };
        String[] groundTruthFilePath = {"data" + File.separator + "cleanCleanErDatasets" + File.separator + "abtBuyIdDuplicates",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "amazonGpIdDuplicates",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "dblpAcmIdDuplicates",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "dblpScholarIdDuplicates",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "moviesIdDuplicates"
        };

        for (int i = 0; i < groundTruthFilePath.length; i++) {
            System.out.println("\n\nCurrent dataset\t:\t" + groundTruthFilePath[i]);

            printDataset(entitiesFilePath[i * 2]);
            printDataset(entitiesFilePath[i * 2 + 1]);
            printGroundtruth(groundTruthFilePath[i]);
        }
    }

}
