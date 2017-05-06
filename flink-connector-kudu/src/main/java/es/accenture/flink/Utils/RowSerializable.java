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

package es.accenture.flink.Utils;


import java.io.Serializable;


public class RowSerializable implements Serializable {
    private static final long serialVersionUID = 1L;
    private Object[] fields2;

    /**
     * Creates an instance of RowSerializable
     * @param arity Size of the row
     */
    public RowSerializable(int arity){

        this.fields2 = new Object[arity];
    }
    public RowSerializable(){

    }

    /**
     * returns number of fields contained in a Row
     * @return int arity
     */
    public int productArity(){
        return this.fields2.length;
    }

    /**
     * Inserts the "field" Object in the position "i".
     * @param i index value
     * @param field Object to write
     */
    public void setField(int i, Object field){
        this.fields2[i]=field;
    }

    /**
     * returns the Object contained in the position "i" from the RowSerializable.
     * @param i index value
     * @return Object
     */
    public Object productElement(int i){
        return this.fields2[i];
    }

    /**
     * returns a String element with the fields of the RowSerializable
     * @return String
     */
    public String toString(){
        String str=fields2[0].toString();
        for (int i=1; i<fields2.length; i++){
            str=str+", " + fields2[i].toString();
        }
        return str;
    }

    public boolean equals(Object object){
        return false;
    }
}
