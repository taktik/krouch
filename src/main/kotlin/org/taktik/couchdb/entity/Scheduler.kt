package org.taktik.couchdb.entity

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.github.pozo.KotlinBuilder
import org.taktik.couchdb.handlers.ReplicationStateDeserializer
import org.taktik.couchdb.handlers.ZonedDateTimeDeserializer
import java.time.ZonedDateTime

interface Scheduler {
    interface ListResult {
        val totalRows: Int
        val offset: Int
    }
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @KotlinBuilder
    data class Docs(
            @JsonProperty("total_rows") override val totalRows: Int,
            override val offset: Int,
            val docs: List<Doc>
    ) : ListResult {
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonIgnoreProperties(ignoreUnknown = true)
        @KotlinBuilder
        data class Doc (
                val database : String? = null,
                @JsonProperty("doc_id") val docId : String? = null,
                val id : String? = null,
                val node : String? = null,
                val source : String? = null,
                val target : String? = null,
                @JsonDeserialize(using = ReplicationStateDeserializer::class) val state : ReplicationState? = null,
                val info : Info? = null,
                @JsonProperty("error_count") val errorCount : Int? = null,
                @JsonProperty("last_updated")
                @JsonDeserialize(using = ZonedDateTimeDeserializer::class)
                val lastUpdated : ZonedDateTime? = null,
                @JsonProperty("start_time")
                @JsonDeserialize(using = ZonedDateTimeDeserializer::class)
                val startTime : ZonedDateTime? = null,
                @JsonProperty("source_proxy") val sourceProxy : String? = null,
                @JsonProperty("target_proxy") val targetProxy : String? = null
        )
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @KotlinBuilder
    data class Info (
            @JsonProperty("revisions_checked") val revisionsChecked : Int? = null,
            @JsonProperty("missing_revisions_found") val missingRevisionsFound : Int? = null,
            @JsonProperty("docs_read") val docsRead : Int? = null,
            @JsonProperty("docs_written") val docsWritten : Int? = null,
            @JsonProperty("changes_pending") val changesPending : Int? = null,
            @JsonProperty("doc_write_failures") val docWriteFailures : Int? = null,
            @JsonProperty("checkpointed_source_seq") val checkpointedSourceSeq : String? = null,
            @JsonProperty("source_seq") val sourceSeq : String? = null,
            @JsonProperty("through_seq") val throughSeq : String? = null,
            val error : String? = null
    )

    interface State {
        val healthy: Boolean
        val terminal: Boolean
    }

    enum class ReplicationState() : State {
        INITIALIZING {
            override val healthy = true
            override val terminal = false
        },
        ERROR {
            override val healthy = false
            override val terminal = false
        },
        FAILED {
            override val healthy = false
            override val terminal = true
        },
        RUNNING {
            override val healthy = true
            override val terminal = false
        },
        PENDING {
            override val healthy = true
            override val terminal = false
        },
        CRASHING {
            override val healthy = false
            override val terminal = false
        },
        COMPLETED {
            override val healthy = true
            override val terminal = true
        };
        companion object {
            fun fromString(value: String?): ReplicationState = when(value) {
                "initializing" -> INITIALIZING
                "error" -> ERROR
                "failed" -> FAILED
                "running" -> RUNNING
                "pending" -> PENDING
                "crashing" -> CRASHING
                "completed" -> COMPLETED
                else -> FAILED
            }
        }

    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @KotlinBuilder
    data class Jobs(
            @JsonProperty("total_rows") override val totalRows: Int,
            override val offset: Int,
            val jobs: List<Job>
    ) : ListResult {
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonIgnoreProperties(ignoreUnknown = true)
        @KotlinBuilder
        data class Job(
                val database: String? = null,
                @JsonProperty("doc_id") val docId: String? = null,
                val id: String? = null,
                val node: String? = null,
                val source: String? = null,
                val target: String? = null,
                val pid: String? = null,
                val user: String? = null,
                val info: Info? = null,
                val history: List<History>? = null,
                @JsonProperty("start_time")
                @JsonDeserialize(using = ZonedDateTimeDeserializer::class)
                val startTime: ZonedDateTime? = null
        ) {
            @JsonInclude(JsonInclude.Include.NON_NULL)
            @JsonIgnoreProperties(ignoreUnknown = true)
            @KotlinBuilder
            data class History(
                    @JsonDeserialize(using = ZonedDateTimeDeserializer::class) val timestamp: ZonedDateTime? = null,
                    val type: String? = null,
                    val reason: String? = null
            )
        }
    }
}
