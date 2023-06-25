/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.connectors.kudu.connector;


import org.apache.flink.connectors.kudu.connector.configuration.type.annotation.ColumnDetail;
import org.apache.flink.connectors.kudu.connector.configuration.type.annotation.StreamingKey;

import java.util.Objects;

public class UserType {
    @StreamingKey(order = 2)
    @ColumnDetail(name = "id_col")
    private Long id;

    @StreamingKey(order = 1)
    @ColumnDetail(name = "name_col")
    private String name;

    @ColumnDetail(name = "age_col")
    private Integer age;

    public UserType() {
    }

    public UserType(Long id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "[name=" + name + ", id=" + id + ", age=" + age + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id, age);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof UserType)) {
            return false;
        } else {
            UserType other = (UserType) obj;
            return ((name == null && other.name == null) || (name != null && name.equals(other.name))) &&
                    ((id == null && other.id == null) || (id != null && id.equals(other.id))) &&
                    ((age == null && other.age == null) || (age != null && age.equals(other.age)));
        }
    }
}
