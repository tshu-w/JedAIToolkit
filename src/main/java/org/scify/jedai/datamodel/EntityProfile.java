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

package org.scify.jedai.datamodel;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

/**
 * The representation of a single entity or record. An entity is comprised of one or more
 * {@link #getAttributes() Attribute} name/value pairs and a {@link #getEntityUrl() URL} which 
 * uniquely identifies each entity.
 *
 * @author G.A.P. II
 */
public class EntityProfile implements Serializable {

    private static final long serialVersionUID = 122354534453243447L;

    private final Set<Attribute> attributes;
    private final String entityUrl;

    public EntityProfile(String url) {
        entityUrl = url;
        attributes = new HashSet<>();
    }

    public void addAttribute(String propertyName, String propertyValue) {
        attributes.add(new Attribute(propertyName, propertyValue));
    }

    /**
     * Add an integer attribute to the profile
     *
     * @param propertyName name of the attribute
     * @param propertyValue value of the attribute
     * @see String#valueOf(int)
     */
    public void addAttribute(String propertyName, int propertyValue) {
        attributes.add(new Attribute(propertyName, String.valueOf(propertyValue)));
    }

    /**
     * Add an attribute to the profile
     *
     * @param propertyName name of the attribute
     * @param propertyValue value of the attribute
     * @see String#valueOf(long)
     */
    public void addAttribute(String propertyName, long propertyValue) {
        attributes.add(new Attribute(propertyName, String.valueOf(propertyValue)));
    }

    /**
     * Add an attribute to the profile
     *
     * @param propertyName name of the attribute
     * @param propertyValue value of the attribute
     * @see String#valueOf(float)
     */
    public void addAttribute(String propertyName, float propertyValue) {
        attributes.add(new Attribute(propertyName, String.valueOf(propertyValue)));
    }

    /**
     * Add an attribute to the profile
     *
     * @param propertyName name of the attribute
     * @param propertyValue value of the attribute
     * @see String#valueOf(double)
     */
    public void addAttribute(String propertyName, double propertyValue) {
        attributes.add(new Attribute(propertyName, String.valueOf(propertyValue)));
    }

    /**
     * Add an attribute to the profile
     *
     * @param propertyName name of the attribute
     * @param propertyValue value of the attribute
     * @see String#valueOf(Object)
     */
    public void addAttribute(String propertyName, Date propertyValue) {
        attributes.add(new Attribute(propertyName, String.valueOf(propertyValue)));
    }

    /**
     * Add an attribute to the profile
     *
     * @param propertyName name of the attribute
     * @param propertyValue local date value of the attribute
     * @see LocalDate#format(DateTimeFormatter)
     */
    public void addAttribute(String propertyName, LocalDate propertyValue) {
        attributes.add(new Attribute(propertyName, propertyValue.format(DateTimeFormatter.ISO_DATE)));
    }

    /**
     * Add an attribute to the profile
     *
     * @param propertyName name of the attribute
     * @param propertyValue local date value of the attribute
     * @see LocalDate#format(DateTimeFormatter)
     */
    public void addAttribute(String propertyName, LocalDate propertyValue, DateTimeFormatter formatter) {
        attributes.add(new Attribute(propertyName, propertyValue.format(formatter)));
    }

    public String getEntityUrl() {
        return entityUrl;
    }

    public int getProfileSize() {
        return attributes.size();
    }
    
    public Set<Attribute> getAttributes() {
        return attributes;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        attributes.forEach((attribute) -> {
            sb.append(attribute.getName()).append(":").append(attribute.getValue()).append(",");
        });
        sb.setLength(sb.length()-1);
        return sb.toString();
    }
}