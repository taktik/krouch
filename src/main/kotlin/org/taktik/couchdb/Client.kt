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

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.util.TokenBuffer
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.google.common.reflect.TypeParameter
import com.google.common.reflect.TypeToken
import io.netty.handler.codec.http.HttpHeaderNames
import kotlinx.collections.immutable.persistentListOf
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.produceIn
import org.apache.http.HttpStatus.SC_CONFLICT
import org.apache.http.HttpStatus.SC_NOT_FOUND
import org.slf4j.LoggerFactory
import org.taktik.couchdb.dao.Option
import org.taktik.couchdb.entity.ActiveTask
import org.taktik.couchdb.entity.AttachmentResult
import org.taktik.couchdb.entity.Change
import org.taktik.couchdb.exception.CouchDbException
import org.taktik.couchdb.entity.ViewQuery
import org.taktik.couchdb.exception.ViewResultException
import org.taktik.couchdb.parser.EndArray
import org.taktik.couchdb.parser.EndObject
import org.taktik.couchdb.parser.FieldName
import org.taktik.couchdb.parser.JsonEvent
import org.taktik.couchdb.parser.NumberValue
import org.taktik.couchdb.parser.StartArray
import org.taktik.couchdb.parser.StartObject
import org.taktik.couchdb.parser.StringValue
import org.taktik.couchdb.parser.copyFromJsonEvent
import org.taktik.couchdb.parser.nextSingleValueAs
import org.taktik.couchdb.parser.nextSingleValueAsOrNull
import org.taktik.couchdb.parser.nextValue
import org.taktik.couchdb.parser.skipValue
import org.taktik.couchdb.parser.split
import org.taktik.couchdb.parser.toJsonEvents
import org.taktik.couchdb.entity.Security
import org.taktik.couchdb.entity.Versionable
import org.taktik.couchdb.exception.CouchDbConflictException
import org.taktik.couchdb.parser.toObject
import org.taktik.net.append
import org.taktik.net.param
import org.taktik.net.params
import org.taktik.net.web.HttpMethod
import org.taktik.net.web.Request
import org.taktik.net.web.WebClient

import java.lang.reflect.Type
import java.nio.ByteBuffer
import java.nio.charset.Charset
import kotlin.math.max
import kotlin.math.min

typealias CouchDbDocument = Versionable<String>

/**
 * An event in the [Flow] returned by [Client.queryView]
 */
sealed class ViewQueryResultEvent

/**
 * This event contains the total number of result of the query
 */
data class TotalCount(val total: Int) : ViewQueryResultEvent()

/**
 * This event contains the offset of the query
 */
data class Offset(val offset: Int) : ViewQueryResultEvent()

/**
 * This event contains the update sequence of the query
 */
data class UpdateSequence(val seq: Long) : ViewQueryResultEvent()

abstract class ViewRow<out K, out V, out T> : ViewQueryResultEvent() {
    abstract val id: String
    abstract val key: K?
    abstract val value: V?
    abstract val doc: T?
}

data class ViewRowWithDoc<K, V, T>(override val id: String, override val key: K?, override val value: V?, override val doc: T) : ViewRow<K, V, T>()
data class ViewRowNoDoc<K, V>(override val id: String, override val key: K?, override val value: V?) : ViewRow<K, V, Nothing>() {
    override val doc: Nothing?
        get() = error("Row has no doc")
}

data class ViewRowWithMissingDoc<K, V>(override val id: String, override val key: K?, override val value: V?) : ViewRow<K, V, Nothing>() {
    override val doc: Nothing?
        get() = error("Doc is missing for this row")
}

private data class BulkUpdateRequest<T : CouchDbDocument>(val docs: Collection<T>, @JsonProperty("all_or_nothing") val allOrNothing: Boolean = false)
private data class BulkDeleteRequest(val docs: Collection<DeleteRequest>, @JsonProperty("all_or_nothing") val allOrNothing: Boolean = false)

data class DeleteRequest(@JsonProperty("_id") val id: String, @JsonProperty("_rev") val rev: String?, @JsonProperty("_deleted") val deleted: Boolean = true)
data class BulkUpdateResult(val id: String, val rev: String?, val ok: Boolean?, val error: String?, val reason: String?)
data class DocIdentifier(val id: String?, val rev: String?)

// Convenience inline methods with reified type params
inline fun <reified K, reified U, reified T> Client.queryViewIncludeDocs(query: ViewQuery): Flow<ViewRowWithDoc<K, U, T>> {
    require(query.isIncludeDocs) { "Query must have includeDocs=true" }
    return queryView(query, K::class.java, U::class.java, T::class.java).filterIsInstance()
}

inline fun <reified K, reified T> Client.queryViewIncludeDocsNoValue(query: ViewQuery): Flow<ViewRowWithDoc<K, Nothing, T>> {
    require(query.isIncludeDocs) { "Query must have includeDocs=true" }
    return queryView(query, K::class.java, Nothing::class.java, T::class.java).filterIsInstance()
}

inline fun <reified V, reified T> Client.queryViewIncludeDocsNoKey(query: ViewQuery): Flow<ViewRowWithDoc<Nothing, V, T>> {
    require(query.isIncludeDocs) { "Query must have includeDocs=true" }
    return queryView(query, Nothing::class.java, V::class.java, T::class.java).filterIsInstance()
}

inline fun <reified K, reified V> Client.queryView(query: ViewQuery): Flow<ViewRowNoDoc<K, V>> {
    require(!query.isIncludeDocs) { "Query must have includeDocs=false" }
    return queryView(query, K::class.java, V::class.java, Nothing::class.java).filterIsInstance()
}

inline fun <reified K> Client.queryViewNoValue(query: ViewQuery): Flow<ViewRowNoDoc<K, Nothing>> {
    require(!query.isIncludeDocs) { "Query must have includeDocs=false" }
    return queryView(query, K::class.java, Nothing::class.java, Nothing::class.java).filterIsInstance()
}

suspend inline fun <reified T : CouchDbDocument> Client.get(id: String): T? = this.get(id, T::class.java)

inline fun <reified T : CouchDbDocument> Client.get(ids: List<String>): Flow<T> = this.get(ids, T::class.java)

suspend inline fun <reified T : CouchDbDocument> Client.create(entity: T): T = this.create(entity, T::class.java)

suspend inline fun <reified T : CouchDbDocument> Client.update(entity: T): T = this.update(entity, T::class.java)

inline fun <reified T : CouchDbDocument> Client.bulkUpdate(entities: List<T>): Flow<BulkUpdateResult> = this.bulkUpdate(entities, T::class.java)

inline fun <reified T : CouchDbDocument> Client.subscribeForChanges(classDiscriminator: String, noinline classProvider: (String) -> Class<T>?, since: String = "now", initialBackOffDelay: Long = 100, backOffFactor: Int = 2, maxDelay: Long = 10000): Flow<Change<T>> =
        this.subscribeForChanges(T::class.java, classDiscriminator, classProvider, since, initialBackOffDelay, backOffFactor, maxDelay)


interface Client {
    // Check if db exists
    suspend fun exists(): Boolean

    // CRUD methods
    suspend fun <T : CouchDbDocument> get(id: String, clazz: Class<T>, vararg options: Option): T?

    suspend fun <T : CouchDbDocument> get(id: String, rev: String, clazz: Class<T>, vararg options: Option): T?
    fun <T : CouchDbDocument> get(ids: Collection<String>, clazz: Class<T>): Flow<T>
    fun <T : CouchDbDocument> getForPagination(ids: Collection<String>, clazz: Class<T>): Flow<ViewQueryResultEvent>
    fun getAttachment(id: String, attachmentId: String, rev: String? = null): Flow<ByteBuffer>
    suspend fun createAttachment(id: String, attachmentId: String, rev: String, contentType: String, data: Flow<ByteBuffer>): String
    suspend fun deleteAttachment(id: String, attachmentId: String, rev: String): String
    suspend fun <T : CouchDbDocument> create(entity: T, clazz: Class<T>): T
    suspend fun <T : CouchDbDocument> update(entity: T, clazz: Class<T>): T
    fun <T : CouchDbDocument> bulkUpdate(entities: Collection<T>, clazz: Class<T>): Flow<BulkUpdateResult>
    suspend fun <T : CouchDbDocument> delete(entity: T): DocIdentifier
    fun <T : CouchDbDocument> bulkDelete(entities: Collection<T>): Flow<BulkUpdateResult>

    // Query
    fun <K, V, T> queryView(query: ViewQuery, keyType: Class<K>, valueType: Class<V>, docType: Class<T>): Flow<ViewQueryResultEvent>

    // Changes observing
    fun <T : CouchDbDocument> subscribeForChanges(
        clazz: Class<T>,
        classDiscriminator: String,
        classProvider: (String) -> Class<T>?,
        since: String = "now",
        initialBackOffDelay: Long = 100,
        backOffFactor: Int = 2,
        maxDelay: Long = 10000
    ): Flow<Change<T>>

    fun <T : CouchDbDocument> get(ids: Flow<String>, clazz: Class<T>): Flow<T>
    fun <T : CouchDbDocument> getForPagination(ids: Flow<String>, clazz: Class<T>): Flow<ViewQueryResultEvent>

    suspend fun activeTasks(): List<ActiveTask>
    suspend fun create(q: Int?, n: Int?): Boolean
    suspend fun security(security: Security): Boolean
}

private const val NOT_FOUND_ERROR = "not_found"
private const val ROWS_FIELD_NAME = "rows"
private const val VALUE_FIELD_NAME = "value"
private const val ID_FIELD_NAME = "id"
private const val ERROR_FIELD_NAME = "error"
private const val KEY_FIELD_NAME = "key"
private const val INCLUDED_DOC_FIELD_NAME = "doc"
private const val TOTAL_ROWS_FIELD_NAME = "total_rows"
private const val OFFSET_FIELD_NAME = "offset"
private const val UPDATE_SEQUENCE_NAME = "update_seq"

@ExperimentalCoroutinesApi
class ClientImpl(private val httpClient: WebClient,
                 private val dbURI: java.net.URI,
                 private val username: String,
                 private val password: String,
                 private val objectMapper: ObjectMapper = ObjectMapper().also { it.registerModule(KotlinModule()) }
) : Client {
    private val log = LoggerFactory.getLogger(javaClass.name)

    override suspend fun create(q: Int?, n: Int?): Boolean {
        val request = newRequest(dbURI.let {
            q?.let { q -> it.param("q", q.toString()) } ?: it
        }.let { n?.let { n -> it.param("n", n.toString()) } ?: it }, "", HttpMethod.PUT)

        val result = request
                .getCouchDbResponse<Map<String, *>?>(true)
        return result?.get("ok") == true
    }

    override suspend fun security(security: Security): Boolean {
        val doc = objectMapper.writerFor(Security::class.java).writeValueAsString(security)

        val request = newRequest(dbURI.append("_security"), doc, HttpMethod.PUT)
        val result = request
                .getCouchDbResponse<Map<String, *>?>(true)
        return result?.get("ok") == true
    }

    override suspend fun exists(): Boolean {
        val request = newRequest(dbURI)
        val result = request
                .getCouchDbResponse<Map<String, *>?>(true)
        return result?.get("db_name") != null
    }

    override suspend fun <T : CouchDbDocument> get(id: String, clazz: Class<T>, vararg options: Option): T? {
        require(id.isNotBlank()) { "Id cannot be blank" }
        val request = newRequest(dbURI.append(id).params(options.map { Pair<String, String>(it.paramName(), "true") }.toMap()))

        return request.getCouchDbResponse(clazz, nullIf404 = true)
    }

    override suspend fun <T : CouchDbDocument> get(id: String, rev: String, clazz: Class<T>, vararg options: Option): T? {
        require(id.isNotBlank()) { "Id cannot be blank" }
        require(rev.isNotBlank()) { "Rev cannot be blank" }
        val request = newRequest(dbURI.append(id).params((listOf("rev" to rev) + options.map { Pair<String, String>(it.paramName(), "true") }).toMap()))

        return request.getCouchDbResponse(clazz, nullIf404 = true)
    }

    private data class AllDocsViewValue(val rev: String)

    @FlowPreview
    @ExperimentalCoroutinesApi
    override fun <T : CouchDbDocument> get(ids: Collection<String>, clazz: Class<T>): Flow<T> {
        return getForPagination(ids, clazz)
                .filterIsInstance<ViewRowWithDoc<String, AllDocsViewValue, T>>()
                .map { it.doc }
    }

    @FlowPreview
    @ExperimentalCoroutinesApi
    override fun <T : CouchDbDocument> get(ids: Flow<String>, clazz: Class<T>): Flow<T> {
        return getForPagination(ids, clazz)
                .filterIsInstance<ViewRowWithDoc<String, AllDocsViewValue, T>>()
                .map { it.doc }
    }

    @FlowPreview
    @ExperimentalCoroutinesApi
    override fun <T : CouchDbDocument> getForPagination(ids: Collection<String>, clazz: Class<T>): Flow<ViewQueryResultEvent> {
        val viewQuery = ViewQuery()
                .allDocs()
                .includeDocs(true)
                .keys(ids)
                .ignoreNotFound(true)
        return queryView(viewQuery, String::class.java, AllDocsViewValue::class.java, clazz)
    }

    @FlowPreview
    @ExperimentalCoroutinesApi
    override fun <T : CouchDbDocument> getForPagination(ids: Flow<String>, clazz: Class<T>): Flow<ViewQueryResultEvent> = flow {
        ids.fold(Pair(persistentListOf<String>(), Triple(0, Integer.MAX_VALUE, 0L)), { acc, id ->
            if (acc.first.size == 100) {
                getForPagination(acc.first, clazz).fold(Pair(persistentListOf(id), acc.second)) { res, it ->
                    when (it) {
                        is ViewRowWithDoc<*, *, *> -> {
                            emit(it)
                            res
                        }
                        is TotalCount -> {
                            Pair(res.first, Triple(res.second.first + it.total, res.second.second, res.second.third))
                        }
                        is Offset -> {
                            Pair(res.first, Triple(res.second.first, min(res.second.second, it.offset), res.second.third))
                        }
                        is UpdateSequence -> {
                            Pair(res.first, Triple(res.second.first, res.second.second, max(res.second.third, it.seq)))
                        }
                        else -> res
                    }
                }
            } else {
                Pair(acc.first.add(id), acc.second)
            }
        }).let { remainder ->
            if (remainder.first.isNotEmpty())
                getForPagination(remainder.first, clazz).fold(remainder.second) { counters, it ->
                    when (it) {
                        is ViewRowWithDoc<*, *, *> -> {
                            emit(it)
                            counters
                        }
                        is TotalCount -> {
                            Triple(counters.first + it.total, counters.second, counters.third)
                        }
                        is Offset -> {
                            Triple(counters.first, min(counters.second, it.offset), counters.third)
                        }
                        is UpdateSequence -> {
                            Triple(counters.first, counters.second, max(counters.third, it.seq))
                        }
                        else -> counters
                    }
                } else remainder.second
        }.let {
            emit(TotalCount(it.first))
            if (it.second < Integer.MAX_VALUE) {
                emit(Offset(it.second))
            }
            if (it.third > 0L) {
                emit(UpdateSequence(it.third))
            }
        }
    }

    @ExperimentalCoroutinesApi
    override fun getAttachment(id: String, attachmentId: String, rev: String?): Flow<ByteBuffer> {
        require(id.isNotBlank()) { "Id cannot be blank" }
        require(attachmentId.isNotBlank()) { "attachmentId cannot be blank" }
        val request = newRequest(dbURI.append(id).append(attachmentId).let { u -> rev?.let { u.param("rev", it) } ?: u })

        return request.retrieve().toBytesFlow()
    }

    override suspend fun deleteAttachment(id: String, attachmentId: String, rev: String): String {
        require(id.isNotBlank()) { "Id cannot be blank" }
        require(attachmentId.isNotBlank()) { "attachmentId cannot be blank" }
        require(rev.isNotBlank()) { "rev cannot be blank" }

        val uri = dbURI.append(id).append(attachmentId)
        val request = newRequest(uri.param("rev", rev), HttpMethod.DELETE)

        return request.getCouchDbResponse<AttachmentResult>()!!.rev
    }


    override suspend fun createAttachment(id: String, attachmentId: String, rev: String, contentType: String, data: Flow<ByteBuffer>): String = coroutineScope {
        require(id.isNotBlank()) { "Id cannot be blank" }
        require(attachmentId.isNotBlank()) { "attachmentId cannot be blank" }
        require(rev.isNotBlank()) { "rev cannot be blank" }

        val uri = dbURI.append(id).append(attachmentId)
        val request = newRequest(uri.param("rev", rev), HttpMethod.PUT).header("Content-type", contentType).body(data)

        request.getCouchDbResponse<AttachmentResult>()!!.rev
    }

    // CouchDB Response body for Create/Update/Delete
    private data class CUDResponse(val id: String, val rev: String, val ok: Boolean)

    override suspend fun <T : CouchDbDocument> create(entity: T, clazz: Class<T>): T {
        val uri = dbURI
        val serializedDoc = objectMapper.writerFor(clazz).writeValueAsString(entity)
        val request = newRequest(uri, serializedDoc)

        val createResponse = request.getCouchDbResponse<CUDResponse>()!!.also {
            check(it.ok)
        }
        // Create a new copy of the doc and set rev/id from response
        @Suppress("BlockingMethodInNonBlockingContext")
        return entity.withIdRev(createResponse.id, createResponse.rev) as T
    }

    override suspend fun <T : CouchDbDocument> update(entity: T, clazz: Class<T>): T {
        val docId = entity.id
        require(docId.isNotBlank()) { "Id cannot be blank" }
        val updateURI = dbURI.append(docId)
        val serializedDoc = objectMapper.writerFor(clazz).writeValueAsString(entity)
        val request = newRequest(updateURI, serializedDoc, HttpMethod.PUT)

        val updateResponse = request.getCouchDbResponse<CUDResponse>()!!.also {
            check(it.ok)
        }
        // Create a new copy of the doc and set rev/id from response
        @Suppress("BlockingMethodInNonBlockingContext")
        return entity.withIdRev(updateResponse.id, updateResponse.rev) as T
    }

    override suspend fun <T : CouchDbDocument> delete(entity: T): DocIdentifier {
        val id = entity.id
        require(!id.isBlank()) { "Id cannot be blank" }
        require(!entity.rev.isNullOrBlank()) { "Revision cannot be blank" }
        val uri = dbURI.append(id).param("rev", entity.rev!!)

        val request = newRequest(uri, HttpMethod.DELETE)

        return request.getCouchDbResponse<CUDResponse>()!!.also {
            check(it.ok)
        }.let { DocIdentifier(it.id, it.rev) }
    }

    @FlowPreview
    @ExperimentalCoroutinesApi
    override fun <T : CouchDbDocument> bulkUpdate(entities: Collection<T>, clazz: Class<T>): Flow<BulkUpdateResult> = flow {
        coroutineScope {
            val updateRequest = BulkUpdateRequest(entities)
            val uri = dbURI.append("_bulk_docs")
            val request = newRequest(uri, objectMapper.writeValueAsString(updateRequest))

            val asyncParser = objectMapper.createNonBlockingByteArrayParser()
            val jsonTokens = request.retrieve().toJsonEvents(asyncParser).produceIn(this)
            check(jsonTokens.receive() === StartArray) { "Expected result to start with StartArray" }
            while (true) { // Loop through result array
                val nextValue = jsonTokens.nextValue(asyncParser) ?: break

                @Suppress("BlockingMethodInNonBlockingContext")
                val bulkUpdateResult = checkNotNull(nextValue.asParser(objectMapper).readValueAs(BulkUpdateResult::class.java))
                emit(bulkUpdateResult)
            }
            jsonTokens.cancel()
        }
    }

    @FlowPreview
    @ExperimentalCoroutinesApi
    override fun <T : CouchDbDocument> bulkDelete(entities: Collection<T>): Flow<BulkUpdateResult> = flow {
        coroutineScope {
            val updateRequest = BulkDeleteRequest(entities.map { DeleteRequest(it.id, it.rev) })
            val uri = dbURI.append("_bulk_docs")
            val request = newRequest(uri, objectMapper.writeValueAsString(updateRequest))

            val asyncParser = objectMapper.createNonBlockingByteArrayParser()
            val jsonEvents = request.retrieve().toJsonEvents(asyncParser).produceIn(this)
            check(jsonEvents.receive() == StartArray) { "Expected result to start with StartArray" }
            while (true) { // Loop through result array
                val nextValue = jsonEvents.nextValue(asyncParser) ?: break

                @Suppress("BlockingMethodInNonBlockingContext")
                val bulkUpdateResult = checkNotNull(nextValue.asParser(objectMapper).readValueAs(BulkUpdateResult::class.java))
                emit(bulkUpdateResult)
            }
            jsonEvents.cancel()
        }
    }

    @FlowPreview
    override fun <K, V, T> queryView(query: ViewQuery, keyType: Class<K>, valueType: Class<V>, docType: Class<T>): Flow<ViewQueryResultEvent> = flow {
        coroutineScope {
            val dbQuery = query.dbPath(dbURI.toString())
            val request = buildRequest(dbQuery)
            val asyncParser = objectMapper.createNonBlockingByteArrayParser()

            /** Execute the request and get the response as a Flow of [JsonEvent] **/
            val jsonEvents = request.retrieve().toJsonEvents(asyncParser).produceIn(this)

            // Response should be a Json object
            val firstEvent = jsonEvents.receive()
            check(firstEvent == StartObject) { "Expected data to start with an Object" }
            resultLoop@ while (true) { // Loop through result object fields
                when (val nextEvent = jsonEvents.receive()) {
                    EndObject -> break@resultLoop // End of result object
                    is FieldName -> {
                        when (nextEvent.name) {
                            ROWS_FIELD_NAME -> { // We found the "rows" field
                                // Rows field should be an array
                                check(jsonEvents.receive() == StartArray) { "Expected rows field to be an array" }
                                // At this point we are in the rows array, and StartArray event has been consumed
                                // We iterate over the rows until we encounter the EndArray event
                                rowsLoop@ while (true) { // Loop through "rows" array
                                    when (jsonEvents.receive()) {
                                        StartObject -> {
                                        } // Start of a new row
                                        EndArray -> break@rowsLoop  // End of rows array
                                        else -> error("Expected Start of new row or end of row array")
                                    }
                                    // At this point we are in a row object, and StartObject event has been consumed.
                                    // We iterate over the field names and construct the ViewRowWithDoc or ViewRowNoDoc Object,
                                    // until we encounter the EndObject event
                                    var id: String? = null
                                    var key: K? = null
                                    var value: V? = null
                                    var doc: T? = null
                                    rowLoop@ while (true) { // Loop through row object fields
                                        when (val nextRowEvent = jsonEvents.receive()) {
                                            EndObject -> break@rowLoop // End of row object
                                            is FieldName -> {
                                                when (nextRowEvent.name) {
                                                    // Parse doc id
                                                    ID_FIELD_NAME -> {
                                                        id = (jsonEvents.receive() as? StringValue)?.value
                                                                ?: error("id field should be a string")
                                                    }
                                                    // Parse key
                                                    KEY_FIELD_NAME -> {
                                                        val keyEvents = jsonEvents.nextValue(asyncParser)
                                                                ?: throw IllegalStateException("Invalid json expecting key")
                                                        @Suppress("BlockingMethodInNonBlockingContext")
                                                        key = keyEvents.asParser(objectMapper).readValueAs(keyType)
                                                    }
                                                    // Parse value
                                                    VALUE_FIELD_NAME -> {
                                                        val valueEvents = jsonEvents.nextValue(asyncParser)
                                                                ?: throw IllegalStateException("Invalid json field name")
                                                        @Suppress("BlockingMethodInNonBlockingContext")
                                                        value = valueEvents.asParser(objectMapper).readValueAs(valueType)
                                                    }
                                                    // Parse doc
                                                    INCLUDED_DOC_FIELD_NAME -> {
                                                        if (dbQuery.isIncludeDocs) {
                                                            jsonEvents.nextValue(asyncParser)?.let {
                                                                doc = it.asParser(objectMapper).readValueAs(docType)
                                                            }
                                                        }
                                                    }
                                                    // Error field
                                                    ERROR_FIELD_NAME -> {
                                                        val error = jsonEvents.nextSingleValueAs<StringValue>()
                                                        val errorMessage = error.value
                                                        if (!ignoreError(dbQuery, errorMessage)) {
                                                            // TODO retrieve key?
                                                            throw ViewResultException(null, errorMessage)
                                                        }
                                                    }
                                                    // Skip other fields values
                                                    else -> jsonEvents.skipValue()
                                                }
                                            }
                                            else -> error("Expected EndObject or FieldName")
                                        }
                                    }
                                    // We finished parsing a row, emit the result
                                    id?.let {
                                        val row: ViewRow<K, V, T> = if (dbQuery.isIncludeDocs) {
                                            if (doc != null) ViewRowWithDoc(it, key, value, doc) as ViewRow<K, V, T> else ViewRowWithMissingDoc(it, key, value)
                                        } else {
                                            ViewRowNoDoc(it, key, value)
                                        }
                                        emit(row)
                                    } ?: if (value is Int) {
                                        emit(ViewRowNoDoc("", key, value))
                                    }

                                }
                            }
                            TOTAL_ROWS_FIELD_NAME -> {
                                val totalValue = jsonEvents.nextSingleValueAs<NumberValue<*>>().value
                                emit(TotalCount(totalValue.toInt()))
                            }
                            OFFSET_FIELD_NAME -> {
                                val offsetValue = jsonEvents.nextSingleValueAsOrNull<NumberValue<*>>()?.value ?: -1
                                emit(Offset(offsetValue.toInt()))
                            }
                            UPDATE_SEQUENCE_NAME -> {
                                val offsetValue = jsonEvents.nextSingleValueAs<NumberValue<*>>().value
                                emit(UpdateSequence(offsetValue.toLong()))
                            }
                            ERROR_FIELD_NAME -> {
                                error("Error executing request $request: ${jsonEvents.nextSingleValueAs<StringValue>().value}")
                            }
                            else -> jsonEvents.skipValue()
                        }
                    }
                    else -> error("Expected EndObject or FieldName, found $nextEvent")
                }
            }
            jsonEvents.cancel()
        }
    }

    @FlowPreview
    override fun <T : CouchDbDocument> subscribeForChanges(
        clazz: Class<T>,
        classDiscriminator: String,
        classProvider: (String) -> Class<T>?,
        since: String,
        initialBackOffDelay: Long,
        backOffFactor: Int,
        maxDelay: Long
    ): Flow<Change<T>> = flow {
        var lastSeq = since
        var delayMillis = initialBackOffDelay
        var changesFlow = internalSubscribeForChanges(clazz, lastSeq, classDiscriminator, classProvider)
        while (true) {
            try {
                changesFlow.collect { change ->
                    lastSeq = change.seq
                    delayMillis = initialBackOffDelay
                    emit(change)
                }
            } catch (e: Exception) {
                if (e is CancellationException) {
                    throw e
                }
                log.warn("Error while listening for changes. Will try to re-subscribe in ${delayMillis}ms", e)
                // Attempt to re-subscribe indefinitely, with an exponential backoff
                delay(delayMillis)
                changesFlow = internalSubscribeForChanges(clazz, lastSeq, classDiscriminator, classProvider)
                delayMillis = (delayMillis * backOffFactor).coerceAtMost(maxDelay)
            }
        }
    }

    override suspend fun activeTasks(): List<ActiveTask> {
        val uri = dbURI.append("_active_tasks")
        val request = newRequest(uri)
        return getCouchDbResponseWithTypeReified(request)!!
    }

    private inline suspend fun <reified T> getCouchDbResponseWithTypeReified(request : Request) : T? {
        return request.getCouchDbResponseWithType(T::class.java, nullIf404 = true)
    }

    @ExperimentalCoroutinesApi
    @FlowPreview
    private fun <T : CouchDbDocument> internalSubscribeForChanges(clazz: Class<T>, since: String, classDiscriminator: String, classProvider: (String) -> Class<T>?): Flow<Change<T>> = flow {
        val charset = Charset.forName("UTF-8")

        log.info("Subscribing for changes of class $clazz")
        val asyncParser = objectMapper.createNonBlockingByteArrayParser()
        // Construct request
        val changesRequest = newRequest(dbURI.append("_changes").param("feed", "continuous")
                .param("heartbeat", "10000")
                .param("include_docs", "true")
                .param("since", since))

        // Get the response as a Flow of CharBuffers (needed to split by line)
        val responseText = changesRequest.retrieve().toTextFlow()
        // Split by line
        val splitByLine = responseText.split('\n')
        // Convert to json events
        val jsonEvents = splitByLine.map {
            it.map {
                charset.encode(it)
            }.toJsonEvents(asyncParser)
        }
        // Parse as generic Change Object
        val changes = jsonEvents.map { events ->
            TokenBuffer(asyncParser).let { tb ->
                var level = 0
                val type = events.foldIndexed(null as String?) { index, type, jsonEvent ->
                    tb.copyFromJsonEvent(jsonEvent)

                    when(jsonEvent) {
                        is FieldName -> if (level == 2 && jsonEvent.name == classDiscriminator && index + 1 < events.size) (events[index + 1] as? StringValue)?.value else type
                        is StartArray -> type.also { level++ }
                        is StartObject -> type.also { level++ }
                        is EndObject -> type.also { level-- }
                        is EndArray -> type.also { level-- }
                        else -> type
                    }
                }
                Pair(type, tb)
            }
        }
        changes.collect { (className, buffer) ->
            if (className != null) {
                val changeClass = classProvider(className)
                if (changeClass != null && clazz.isAssignableFrom(changeClass)) {
                    val coercedClass = changeClass as Class<T>
                    val changeType = object : TypeToken<Change<T>>() {}.where(object : TypeParameter<T>() {}, coercedClass).type
                    val typeRef = object : TypeReference<Change<T>>() {
                        override fun getType(): Type {
                            return changeType
                        }
                    }
                    @Suppress("UNCHECKED_CAST")
                    // Parse as actual Change object with the correct class
                    emit(buffer.asParser(objectMapper).readValueAs<Change<T>>(typeRef))
                }
            }
        }
    }

    private fun newRequest(uri: java.net.URI, method: HttpMethod = HttpMethod.GET) = httpClient.uri(uri).method(method).basicAuth(username, password)
    private fun newRequest(uri: java.net.URI, body: String, method: HttpMethod = HttpMethod.POST) = newRequest(uri, method)
            .header(HttpHeaderNames.CONTENT_TYPE.toString(), "application/json")
            .body(body)

    private fun newRequest(uri: String, method: HttpMethod = HttpMethod.GET) = httpClient.uri(uri).method(method).basicAuth(username, password)
    private fun newRequest(uri: String, body: String, method: HttpMethod = HttpMethod.POST) = newRequest(uri, method)
            .header(HttpHeaderNames.CONTENT_TYPE.toString(), "application/json")
            .body(body)

    private fun buildRequest(query: ViewQuery) =
            if (query.hasMultipleKeys()) {
                newRequest(query.buildQuery(), query.keysAsJson())
            } else {
                newRequest(query.buildQuery())
            }

    private fun ignoreError(query: ViewQuery, error: String): Boolean {
        return query.ignoreNotFound && NOT_FOUND_ERROR == error
    }

    suspend fun <T> Request.getCouchDbResponse(clazz: Class<T>, emptyResponseAsNull: Boolean = false, nullIf404: Boolean = false): T? = this.getCouchDbResponseWithType(clazz, emptyResponseAsNull, nullIf404)
    suspend fun <T> Request.getCouchDbResponseWithType(type: Class<T>, emptyResponseAsNull: Boolean = false, nullIf404: Boolean = false): T? {
        return try {
            return this
                    .retrieve()
                    .onStatus(SC_NOT_FOUND) { response -> throw CouchDbException("Not found", response.statusCode, response.responseBodyAsString()) }
                    .onStatus(SC_CONFLICT) { response -> throw CouchDbConflictException("Conflict", response.statusCode, response.responseBodyAsString()) }
                    .toFlow()
                    .toObject(type, objectMapper, emptyResponseAsNull)
        } catch (ex : CouchDbException) {
            if (ex.statusCode == 404 && nullIf404) null else throw ex
        }
    }

    private suspend inline fun <reified T> Request.getCouchDbResponse(nullIf404: Boolean = false): T? = getCouchDbResponse(T::class.java, null is T, nullIf404)

    private data class CouchDbErrorResponse(val error: String? = null, val reason: String? = null)

}

