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

package org.taktik.couchdb.dao

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import org.taktik.couchdb.Client
import org.taktik.couchdb.Code
import org.taktik.couchdb.ViewRowWithDoc
import org.taktik.couchdb.annotation.View
import org.taktik.couchdb.annotation.Views
import org.taktik.couchdb.dao.impl.GenericDAOImpl
import org.taktik.couchdb.entity.ComplexKey
import org.taktik.couchdb.entity.ViewQuery
import org.taktik.couchdb.queryViewIncludeDocs

@Views(View(name = "all", map = "function(doc) { if (doc.java_type == 'Code' && !doc.deleted) emit(null, doc._id )}"),
       View(name = "by_type", map = "classpath:js/code/by_type.js"))
class CodeDAO(client: Client) : GenericDAOImpl<Code>(Code::class.java, client) {

    @ExperimentalCoroutinesApi
    @FlowPreview
    @View(name = "by_type_version", map = "classpath:js/code/by_type_version.js")
    fun findCodeByTypeAndVersion(type: String, version: String): Flow<ViewRowWithDoc<List<*>, String, Code>> {
        require(type.isNotBlank()) { "type cannot be blank" }
        require(version.isNotBlank()) { "version cannot be blank" }

        val queryView = ViewQuery()
            .designDocId(this.designDocumentId)
            .viewName("by_type_version")
            .key(ComplexKey(arrayOf(type, version)))
            .limit(50)
            .includeDocs(true)

        return client.queryViewIncludeDocs(queryView)
    }
}