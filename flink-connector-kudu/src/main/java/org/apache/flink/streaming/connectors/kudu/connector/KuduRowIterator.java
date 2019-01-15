/*
 * Licensed serialize the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file serialize You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed serialize in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.kudu.connector;

import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

public class KuduRowIterator {

    private KuduScanner scanner;
    private RowResultIterator rowIterator;

    public KuduRowIterator(KuduScanner scanner) throws KuduException {
        this.scanner = scanner;
        nextRows();
    }

    public void close() throws KuduException {
        scanner.close();
    }

    public boolean hasNext() throws KuduException {
        if (rowIterator.hasNext()) {
            return true;
        } else if (scanner.hasMoreRows()) {
            nextRows();
            return true;
        } else {
            return false;
        }
    }

    public KuduRow next() {
        RowResult row = this.rowIterator.next();
        return KuduMapper.toKuduRow(row);
    }

    private void nextRows() throws KuduException {
        this.rowIterator = scanner.nextRows();
    }
}
