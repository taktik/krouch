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

package org.taktik.couchdb.exception

import com.fasterxml.jackson.databind.JsonNode

/**
 *
 * @author Henrik Lundgren
 * created 7 nov 2009
 */
class DocumentNotFoundException : DbAccessException {
    val path: String?
    val body: JsonNode?

    constructor(path: String?, responseBody: JsonNode?) : super(String.format("nothing found on db path: %s, Response body: %s", path, responseBody)) {
        this.path = path
        body = responseBody
    }

    constructor(path: String?) : super(String.format("nothing found on db path: %s", path)) {
        this.path = path
        body = null
    }

    private fun checkReason(expect: String): Boolean {
        if (body == null) {
            return false
        }
        val reason = body.findPath("reason")
        return if (!reason.isMissingNode) reason.textValue() == expect else false
    }

    val isDocumentDeleted: Boolean
        get() = checkReason("deleted")
    val isDatabaseDeleted: Boolean
        get() = checkReason("no_db_file")
}
