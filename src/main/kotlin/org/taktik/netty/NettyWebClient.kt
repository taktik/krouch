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

package org.taktik.netty

import io.netty.buffer.Unpooled.EMPTY_BUFFER
import io.netty.buffer.Unpooled.wrappedBuffer
import io.netty.handler.codec.http.DefaultHttpHeaders
import io.netty.handler.codec.http.HttpHeaders
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactor.asFlux
import org.reactivestreams.Publisher
import org.taktik.net.web.HttpMethod
import org.taktik.net.web.Request
import org.taktik.net.web.Response
import org.taktik.net.web.ResponseStatus
import org.taktik.net.web.WebClient
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import java.net.URI
import java.nio.ByteBuffer

class NettyWebClient : WebClient {
    override fun uri(uri: URI): Request {
        return NettyRequest(HttpClient.create(), uri)
    }
}

class NettyRequest(
    private val client: HttpClient,
    private val uri: URI,
    private val method: HttpMethod? = null,
    private val headers: HttpHeaders = DefaultHttpHeaders(),
    private val bodyPublisher: Flow<ByteBuffer>? = null
) : Request {
    override fun method(method: HttpMethod): Request = NettyRequest(client, uri, method, headers, bodyPublisher)
    override fun header(name: String, value: String): Request = NettyRequest(client, uri, method, headers.add(name, value), bodyPublisher)
    override fun body(producer: Flow<ByteBuffer>): Request = NettyRequest(client, uri, method, headers, producer)
    override fun retrieve(): Response = NettyResponse(
        when (method) {
            HttpMethod.GET -> client.headers { it.add(headers) }.get().uri(uri)
            HttpMethod.HEAD -> client.headers { it.add(headers) }.head().uri(uri)
            HttpMethod.DELETE -> client.headers { it.add(headers) }.delete().uri(uri)
            HttpMethod.OPTIONS -> client.headers { it.add(headers) }.options().uri(uri)
            HttpMethod.POST -> client.headers { it.add(headers) }.post().uri(uri)
                .send(bodyPublisher?.map { wrappedBuffer(it) }?.asFlux() ?: Mono.just(EMPTY_BUFFER))
            HttpMethod.PUT -> client.headers { it.add(headers) }.put().uri(uri)
                .send(bodyPublisher?.map { wrappedBuffer(it) }?.asFlux() ?: Mono.just(EMPTY_BUFFER))
            HttpMethod.PATCH -> client.headers { it.add(headers) }.put().uri(uri)
                .send(bodyPublisher?.map { wrappedBuffer(it) }?.asFlux() ?: Mono.just(EMPTY_BUFFER))
            else -> throw IllegalStateException("Invalid HTTP method")
        }
    )
}

class NettyResponse(private val responseReceiver: HttpClient.ResponseReceiver<*>, private val statusHandlers: Map<Int,(ResponseStatus) -> Mono<out Throwable>> = mapOf()) : Response {
    override fun toFlux(): Publisher<ByteBuffer> {
        return responseReceiver.response { clientResponse, flux ->
            val code = clientResponse.status().code()
            statusHandlers[code]?.let {
                flux.aggregate().asByteArray().flatMapMany { bytes ->
                    val res = it(object : ResponseStatus(code) {
                        override fun responseBodyAsString() = bytes.toString(Charsets.UTF_8)
                    })
                    if (res == Mono.empty<Throwable>()) Mono.just(ByteBuffer.wrap(bytes)) else res.flatMap { Mono.error(it) }
                }
            } ?: flux.map {
                val ba = ByteArray(it.readableBytes())
                it.readBytes(ba) //Bytes need to be read now, before they become unavailable. If we just return the nioBuffer(), we have no guarantee that the bytes will be the same when the ByteBuffer will be processed down the flux
                ByteBuffer.wrap(ba)
            }
        }
    }

    override fun onStatus(status: Int, handler: (ResponseStatus) -> Mono<out Throwable>): Response {
        return NettyResponse(responseReceiver, statusHandlers + (status to handler))
    }
}
