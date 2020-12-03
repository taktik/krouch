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

package org.taktik.couchdb.entity

import org.taktik.couchdb.id.Identifiable

/**
 * @param <T> The type of the entity identity (a String, a UUID, etc.)
</T> */
interface Versionable<T> : Identifiable<T> {
    val revHistory: Map<String, String>?
    val rev: String?

    fun withIdRev(id: T? = null, rev: String): Versionable<T>
}
