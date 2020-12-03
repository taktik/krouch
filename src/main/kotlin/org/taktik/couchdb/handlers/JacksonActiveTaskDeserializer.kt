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

package org.taktik.couchdb.handlers

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.ObjectCodec
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import org.taktik.couchdb.entity.ActiveTask
import org.taktik.couchdb.entity.DatabaseCompactionTask
import org.taktik.couchdb.entity.Indexer
import org.taktik.couchdb.entity.ViewCompactionTask
import org.taktik.couchdb.jackson.JsonObjectDeserializer

class JacksonActiveTaskDeserializer : JsonObjectDeserializer<ActiveTask>() {
    private val discriminator = "type"
    private val subclasses = mapOf(
            "indexer" to Indexer::class.java,
            "replication" to Indexer::class.java,
            "database_compaction" to DatabaseCompactionTask::class.java,
            "view_compaction" to ViewCompactionTask::class.java
    )

    override fun deserializeObject(jsonParser: JsonParser, context: DeserializationContext, codec: ObjectCodec, tree: JsonNode): ActiveTask {
        val discr = tree[discriminator].textValue() ?: throw IllegalArgumentException("Missing discriminator $discriminator in object")
        val selectedSubClass = subclasses[discr] ?: throw IllegalArgumentException("Invalid subclass $discr in object")
        return codec.treeToValue(tree, selectedSubClass)
    }
}
