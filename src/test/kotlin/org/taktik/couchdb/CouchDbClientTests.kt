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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.taktik.net.web.HttpMethod
import org.taktik.couchdb.entity.ViewQuery
import org.taktik.couchdb.exception.CouchDbConflictException
import org.taktik.couchdb.parser.EndArray
import org.taktik.couchdb.parser.StartArray
import org.taktik.couchdb.parser.StartObject
import org.taktik.couchdb.parser.split
import org.taktik.couchdb.parser.toJsonEvents
import org.taktik.netty.NettyWebClient
import java.net.URI
import java.net.URL
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID

@FlowPreview
@ExperimentalCoroutinesApi
class CouchDbClientTests {

    private val databaseHost = System.getProperty("icure.test.couchdb.server.url")
    private val databaseName = System.getProperty("icure.test.couchdb.database.name")
    private val userName = System.getProperty("icure.test.couchdb.username")
    private val password = System.getProperty("icure.test.couchdb.password")

    private val testResponseAsString = URL("https://jsonplaceholder.typicode.com/posts").openStream().use { it.readBytes().toString(StandardCharsets.UTF_8) }
    private val httpClient = NettyWebClient()
    private val client = ClientImpl(
        httpClient,
            URI("$databaseHost/$databaseName"),
            userName,
            password)

    @Test
    fun testSubscribeChanges() = runBlocking {
        val testSize = 10
        val deferredChanges = async {
            client.subscribeForChanges<Code>("java_type", { if (it == "Code") Code::class.java else null }).take(testSize).toList()
        }
        // Wait a bit before updating DB
        delay(3000)
        val codes = List(testSize) { Code.from("test", UUID.randomUUID().toString(), "test") }
        val createdCodes = client.bulkUpdate(codes).toList()
        val changes = deferredChanges.await()
        assertEquals(createdCodes.size, changes.size)
        assertEquals(createdCodes.map { it.id }.toSet(), changes.map { it.id }.toSet())
        assertEquals(codes.map { it.code }.toSet(), changes.map { it.doc.code }.toSet())
    }

    @Test
    fun testExists() = runBlocking {
        assertTrue(client.exists())
    }

    @Test
    fun testExists2() = runBlocking {
        val client = ClientImpl(
                httpClient,
                URI("$databaseHost/${UUID.randomUUID()}"),
                userName,
                password)
        assertFalse(client.exists())
    }

    @Test
    fun testRequestGetResponseBytesFlow() = runBlocking {
        val bytesFlow = httpClient.uri("https://jsonplaceholder.typicode.com/posts").method(HttpMethod.GET).retrieve().toBytesFlow()

        val bytes = bytesFlow.fold(ByteBuffer.allocate(1000000), { acc, buffer -> acc.put(buffer) })
        bytes.flip()
        val responseAsString = StandardCharsets.UTF_8.decode(bytes).toString()
        assertEquals(testResponseAsString, responseAsString)
    }

    @Test
    fun testRequestGetText() = runBlocking {
        val charBuffers = httpClient.uri("https://jsonplaceholder.typicode.com/posts").method(HttpMethod.GET).retrieve().toTextFlow()
        val chars = charBuffers.toList().fold(CharBuffer.allocate(1000000), { acc, buffer -> acc.put(buffer) })
        chars.flip()
        assertEquals(testResponseAsString, chars.toString())
    }

    @Test
    fun testRequestGetTextAndSplit() = runBlocking {
        val charBuffers = httpClient.uri("https://jsonplaceholder.typicode.com/posts").method(HttpMethod.GET).retrieve().toTextFlow()
        val split = charBuffers.split('\n')
        val lines = split.map { it.fold(CharBuffer.allocate(100000), { acc, buffer -> acc.put(buffer) }).flip().toString() }.toList()
        assertEquals(testResponseAsString.split("\n"), lines)
    }

    @Test
    fun testRequestGetJsonEvent() = runBlocking {
        val asyncParser = ObjectMapper().also { it.registerModule(KotlinModule()) }.createNonBlockingByteArrayParser()

        val bytes = httpClient.uri("https://jsonplaceholder.typicode.com/posts").method(HttpMethod.GET).retrieve().toBytesFlow()
        val jsonEvents = bytes.toJsonEvents(asyncParser).toList()
        assertEquals(StartArray, jsonEvents.first(), "Should start with StartArray")
        assertEquals(StartObject, jsonEvents[1], "jsonEvents[1] == StartObject")
        assertEquals(EndArray, jsonEvents.last(), "Should end with EndArray")
    }

    @Test
    fun testClientQueryViewIncludeDocs() = runBlocking {
        val limit = 5
        val query = ViewQuery()
                .designDocId("_design/Code")
                .viewName("all")
                .limit(limit)
                .includeDocs(true)
        val flow = client.queryViewIncludeDocs<String, String, Code>(query)
        val codes = flow.toList()
        assertEquals(limit, codes.size)
    }

    @Test
    fun testClientQueryViewNoDocs() = runBlocking {
        val limit = 5
        val query = ViewQuery()
                .designDocId("_design/Code")
                .viewName("all")
                .limit(limit)
                .includeDocs(false)
        val flow = client.queryView<String, String>(query)
        val codes = flow.toList()
        assertEquals(limit, codes.size)
    }

    @Test
    fun testRawClientQuery() = runBlocking {
        val limit = 5
        val query = ViewQuery()
                .designDocId("_design/Code")
                .viewName("all")
                .limit(limit)
                .includeDocs(false)
        val flow = client.queryView(query, String::class.java, String::class.java, Nothing::class.java)

        val events = flow.toList()
        assertEquals(1, events.filterIsInstance<TotalCount>().size)
        assertEquals(1, events.filterIsInstance<Offset>().size)
        assertEquals(limit, events.filterIsInstance<ViewRow<*, *, *>>().size)
    }

    @Test
    fun testClientGetNonExisting() = runBlocking {
        val nonExistingId = UUID.randomUUID().toString()
        val code = client.get<Code>(nonExistingId)
        assertNull(code)
    }

    @Test
    fun testClientCreateAndGet() = runBlocking {
        val randomCode = UUID.randomUUID().toString()
        val toCreate = Code.from("test", randomCode, "test")
        val created = client.create(toCreate)
        assertEquals(randomCode, created.code)
        assertNotNull(created.id)
        assertNotNull(created.rev)
        val fetched = checkNotNull(client.get<Code>(created.id)) { "Code was just created, it should exist" }
        assertEquals(fetched.id, created.id)
        assertEquals(fetched.code, created.code)
        assertEquals(fetched.rev, created.rev)
    }

    @Test
    fun testClientUpdate() = runBlocking {
        val randomCode = UUID.randomUUID().toString()
        val toCreate = Code.from("test", randomCode, "test")
        val created = client.create(toCreate)
        assertEquals(randomCode, created.code)
        assertNotNull(created.id)
        assertNotNull(created.rev)
        // update code
        val anotherRandomCode = UUID.randomUUID().toString()
        val updated = client.update(created.copy(code = anotherRandomCode))
        assertEquals(created.id, updated.id)
        assertEquals(anotherRandomCode, updated.code)
        assertNotEquals(created.rev, updated.rev)
        val fetched = checkNotNull(client.get<Code>(updated.id))
        assertEquals(fetched.id, updated.id)
        assertEquals(fetched.code, updated.code)
        assertEquals(fetched.rev, updated.rev)
    }

    @Test
    fun testClientUpdateOutdated() {
        Assertions.assertThrows(CouchDbConflictException::class.java) {
            runBlocking {
                val randomCode = UUID.randomUUID().toString()
                val toCreate = Code.from("test", randomCode, "test")
                val created = client.create(toCreate)
                assertEquals(randomCode, created.code)
                assertNotNull(created.id)
                assertNotNull(created.rev)
                // update code
                val anotherRandomCode = UUID.randomUUID().toString()
                val updated = client.update(created.copy(code = anotherRandomCode))
                assertEquals(created.id, updated.id)
                assertEquals(anotherRandomCode, updated.code)
                assertNotEquals(created.rev, updated.rev)
                val fetched = checkNotNull(client.get<Code>(updated.id))
                assertEquals(fetched.id, updated.id)
                assertEquals(fetched.code, updated.code)
                assertEquals(fetched.rev, updated.rev)
                // Should throw a Document update conflict Exception
                @Suppress("UNUSED_VARIABLE")
                val updateResult = client.update(created)
            }
        }
    }

    @Test
    fun testClientDelete() = runBlocking {
        val randomCode = UUID.randomUUID().toString()
        val toCreate = Code.from("test", randomCode, "test")
        val created = client.create(toCreate)
        assertEquals(randomCode, created.code)
        assertNotNull(created.id)
        assertNotNull(created.rev)
        val deletedRev = client.delete(created)
        assertNotEquals(created.rev, deletedRev)
        assertNull(client.get<Code>(created.id))
    }

    @Test
    fun testClientBulkGet() = runBlocking {
        val limit = 100
        val query = ViewQuery()
                .designDocId("_design/Code")
                .viewName("by_language_type_label")
                .limit(limit)
                .includeDocs(true)
        val flow = client.queryViewIncludeDocs<List<*>, Int, Code>(query)
        val codes = flow.map { it.doc }.toList()
        val codeIds = codes.map { it.id }
        val flow2 = client.get<Code>(codeIds)
        val codes2 = flow2.toList()
        assertEquals(codes, codes2)
    }

    @Test
    fun testClientBulkUpdate() = runBlocking {
        val testSize = 100
        val codes = List(testSize) { Code.from("test", UUID.randomUUID().toString(), "test") }
        val updateResult = client.bulkUpdate(codes).toList()
        assertEquals(testSize, updateResult.size)
        assertTrue(updateResult.all { it.error == null })
        val revisions = updateResult.map { checkNotNull(it.rev) }
        val ids = codes.map { it.id }
        val codeCodes = codes.map { it.code }
        val fetched = client.get<Code>(ids).toList()
        assertEquals(codeCodes, fetched.map { it.code })
        assertEquals(revisions, fetched.map { it.rev })
    }


}
