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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty

/**
 *
 * @author henrik lundgren
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
class Attachment(
        @JsonIgnore val id: String? = null,
        @field:JsonProperty("content_type") val contentType: String? = null,
        @JsonIgnore val contentLength: Long = 0,
        @field:JsonProperty("data") val dataBase64: String? = null,
        val isStub: Boolean = false,
        val revpos: Int = 0,
        val digest: String? = null
)
