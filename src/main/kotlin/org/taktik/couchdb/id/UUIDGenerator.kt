/*
 *    Copyright 2020 Taktik SA
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
 *
 */

package org.taktik.couchdb.id

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID

class UUIDGenerator : IDGenerator {

    @Synchronized
    override fun incrementAndGet(sequenceName: String): Int {
        throw IllegalStateException("Not supported")
    }

    override fun newGUID(): UUID {
        return UUID.randomUUID()
    }
}
