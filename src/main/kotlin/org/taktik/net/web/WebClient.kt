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

package org.taktik.net.web

import com.fasterxml.jackson.core.JsonParser
import io.netty.handler.codec.http.HttpHeaderNames
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.reactive.asFlow
import org.reactivestreams.Publisher
import org.taktik.couchdb.parser.toJsonEvents
import reactor.core.publisher.Mono
import java.net.URI
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.Charset
import java.nio.charset.CoderResult
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Base64
import kotlin.math.roundToInt

interface WebClient {
    fun uri(uri: URI): Request
    fun uri(uri: String) = uri(URI(uri))
}

interface Request {
    fun method(method: HttpMethod, timeoutDuration: Duration? = null): Request
    fun basicAuth(username: String, password: String): Request {
        return header(
            HttpHeaderNames.AUTHORIZATION.toString(),
            "Basic ${Base64.getEncoder().encode("${username}:${password}".toByteArray(Charsets.UTF_8)).toString(Charsets.UTF_8)}"
        )
    }

    fun header(name: String, value: String): Request
    fun body(producer: Publisher<ByteBuffer>) = body(producer.asFlow())
    fun body(producer: Flow<ByteBuffer>): Request
    fun body(text: String) = this.body(Mono.just(ByteBuffer.wrap(text.toByteArray(Charsets.UTF_8))).asFlow())

    fun retrieve(): Response
}

interface Response {
    fun toFlux(): Publisher<ByteBuffer>
    fun toFlow() = toFlux().asFlow()
    fun onStatus(status: Int, handler: (ResponseStatus) -> Mono<out Throwable>): Response

    /**
    Execute this WebClient [WebClient.RequestHeadersSpec] and get the response as a [Flow] of [ByteBuffer].
     */
    fun toBytesFlow(buffer: Int = 1) = toFlow().buffer(buffer)
    fun toJsonEvents(asyncParser: JsonParser, buffer: Int = 1) = toBytesFlow(buffer).toJsonEvents(asyncParser)
    fun toTextFlow(charset: Charset = StandardCharsets.UTF_8, buffer: Int = 1): Flow<CharBuffer> = flow<CharBuffer> {
        val decoder = charset.newDecoder()
        var remainingBytes: ByteBuffer? = null
        var skip = false
        var error: Exception? = null

        toFlow().collect { bb ->
            if (!skip) {
                var cb = CharBuffer.allocate(
                    ((bb.remaining() + (remainingBytes?.remaining() ?: 0)) * decoder.averageCharsPerByte()).roundToInt()
                )

                var coderResult = decoder.decode(remainingBytes?.let {
                    ByteBuffer.allocate(it.remaining() + bb.remaining()).apply {
                        put(it)
                        put(bb)
                    }.flip()
                } ?: bb, cb, false)

                while (coderResult.isOverflow) {
                    cb.flip()
                    emit(cb)
                    cb = CharBuffer.allocate((bb.remaining() * decoder.averageCharsPerByte()).roundToInt())
                    coderResult = decoder.decode(bb, cb, false)
                }

                remainingBytes = when (coderResult) {
                    CoderResult.UNDERFLOW -> {
                        cb.flip()
                        emit(cb)
                        if (bb.hasRemaining()) {
                            ByteBuffer.allocate(bb.remaining()).apply {
                                put(bb)
                                flip()
                            }
                        } else null
                    }
                    else -> {
                        error = IllegalStateException("Error decoding response : $coderResult")
                        skip = true
                        null
                    }
                }
            }
        }

        error?.let { throw it }

        remainingBytes?.let {
            if (it.hasRemaining()) {
                val cb = CharBuffer.allocate((it.remaining() * decoder.averageCharsPerByte()).roundToInt())
                decoder.decode(it, cb, true)
                cb.flip()
                emit(cb)
            }
        }
    }.buffer(buffer)
}

abstract class ResponseStatus(val statusCode: Int) {
    abstract fun responseBodyAsString(): String
}

enum class HttpMethod {
    GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS;
}
