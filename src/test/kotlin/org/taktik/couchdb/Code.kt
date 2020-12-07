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
package org.taktik.couchdb

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.github.pozo.KotlinBuilder
import org.taktik.couchdb.entity.Attachment

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@KotlinBuilder
data class Code(
        @JsonProperty("_id") override val id: String,         // id = type|code|version  => this must be unique
        @JsonProperty("_rev") override val rev: String? = null,
        @JsonProperty("deleted") val deletionDate: Long? = null,

        val context: String? = null, //ex: When embedded the context where this code is used
        val type: String? = null, //ex: ICD (type + version + code combination must be unique) (or from tags -> CD-ITEM)
        val code: String? = null, //ex: I06.2 (or from tags -> healthcareelement). Local codes are encoded as LOCAL:SLLOCALFROMMYSOFT
        val version: String? = null, //ex: 10. Must be lexicographically searchable
        val label: Map<String, String> = mapOf(), //ex: {en: Rheumatic Aortic Stenosis, fr: Sténose rhumatoïde de l'Aorte}
        val author: String? = null,
        val regions: Set<String> = setOf(), //ex: be,fr
        val level: Int? = null, //ex: 0 = System, not to be modified by user, 1 = optional, created or modified by user
        val links: Set<String> = setOf(), //Links towards related codes (corresponds to an approximate link in qualifiedLinks)

        @JsonProperty("_attachments") val attachments: Map<String, Attachment>? = null,
        @JsonProperty("_conflicts") val conflicts: List<String>? = null,
        @JsonProperty("rev_history") override val revHistory: Map<String, String>? = null,
        @JsonProperty("java_type") val javaType: String = "Code"
) : CouchDbDocument {
    companion object {
        fun from(type: String, code: String, version: String) = Code(id = "$type:$code:$version", type = type, code = code, version = version)
    }
    override fun withIdRev(id: String?, rev: String) = if (id != null) this.copy(id = id, rev = rev) else this.copy(rev = rev)
}
