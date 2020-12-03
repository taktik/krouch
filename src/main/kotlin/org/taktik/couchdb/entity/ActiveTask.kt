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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import org.taktik.couchdb.handlers.InstantDeserializer
import org.taktik.couchdb.handlers.InstantSerializer
import org.taktik.couchdb.handlers.JacksonActiveTaskDeserializer
import java.time.Instant

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(using = JacksonActiveTaskDeserializer::class)
@JsonIgnoreProperties(ignoreUnknown = true)
sealed class ActiveTask(
        val pid: String? = null,
        @JsonSerialize(using = InstantSerializer::class)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonDeserialize(using = InstantDeserializer::class)
        val started_on: Instant? = null,
        @JsonSerialize(using = InstantSerializer::class)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonDeserialize(using = InstantDeserializer::class)
        val updated_on: Instant? = null
)

@Suppress("unused")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(using = JsonDeserializer.None::class)
@JsonIgnoreProperties(ignoreUnknown = true)
class UnsupportedTask(
        pid: String? = null,
        val progress: Int? = null,
        @JsonSerialize(using = InstantSerializer::class)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonDeserialize(using = InstantDeserializer::class)
        started_on: Instant? = null,
        @JsonSerialize(using = InstantSerializer::class)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonDeserialize(using = InstantDeserializer::class)
        updated_on: Instant? = null
) : ActiveTask(pid, started_on, updated_on)

@Suppress("unused")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(using = JsonDeserializer.None::class)
@JsonIgnoreProperties(ignoreUnknown = true)
class DatabaseCompactionTask(
        pid: String? = null,
        val progress: Int? = null,
        @JsonSerialize(using = InstantSerializer::class)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonDeserialize(using = InstantDeserializer::class)
        started_on: Instant? = null,
        @JsonSerialize(using = InstantSerializer::class)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonDeserialize(using = InstantDeserializer::class)
        updated_on: Instant? = null,
        val database: String?,
        val total_changes: Double?,
        val completed_changes: Double?
) : ActiveTask(pid, started_on, updated_on)

@Suppress("unused")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(using = JsonDeserializer.None::class)
@JsonIgnoreProperties(ignoreUnknown = true)
class ViewCompactionTask(
        pid: String? = null,
        val progress: Int? = null,
        @JsonSerialize(using = InstantSerializer::class)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonDeserialize(using = InstantDeserializer::class)
        started_on: Instant? = null,
        @JsonSerialize(using = InstantSerializer::class)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonDeserialize(using = InstantDeserializer::class)
        updated_on: Instant? = null,
        val database: String?,
        val design_document: String?,
        val phase: String?,
        val total_changes: Double?,
        val view: Double?,
        val completed_changes: Double?
) : ActiveTask(pid, started_on, updated_on)

@Suppress("unused")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(using = JsonDeserializer.None::class)
@JsonIgnoreProperties(ignoreUnknown = true)
class Indexer(
        pid: String? = null,
        val progress: Int? = null,
        @JsonSerialize(using = InstantSerializer::class)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonDeserialize(using = InstantDeserializer::class)
        started_on: Instant? = null,
        @JsonSerialize(using = InstantSerializer::class)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonDeserialize(using = InstantDeserializer::class)
        updated_on: Instant? = null,
        val database: String?,
        val node: String?,
        val design_document: String?,
        val total_changes: Double?,
        val completedChanges: Double?
) : ActiveTask(pid, started_on, updated_on)

@Suppress("unused")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(using = JsonDeserializer.None::class)
@JsonIgnoreProperties(ignoreUnknown = true)
class ReplicationTask(
        pid: String? = null,
        @JsonSerialize(using = InstantSerializer::class)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonDeserialize(using = InstantDeserializer::class)
        started_on: Instant? = null,
        @JsonSerialize(using = InstantSerializer::class)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonDeserialize(using = InstantDeserializer::class)
        updated_on: Instant? = null,
        val replication_id: String?,
        val doc_id: String?,
        val node: String?,
        val continuous: Boolean,
        val changes_pending: Double?,
        val doc_write_failures: Double,
        val docs_read: Double,
        val docs_written: Double,
        val missing_revisions_found: Double,
        val revisions_checked: Double,
        val source: String?,
        val target: String?,
        val source_seq: String?,
        val checkpointed_source_seq: String?,
        val checkpoint_interval: Double
) : ActiveTask(pid, started_on, updated_on)
