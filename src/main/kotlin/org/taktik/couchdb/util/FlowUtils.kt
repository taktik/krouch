package org.taktik.couchdb.util

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect

@ExperimentalCoroutinesApi
fun <T> Flow<T>.bufferedChunks(min: Int, max: Int): Flow<List<T>> = channelFlow {
    require(min >= 1 && max >= 1 && max >= min) {
        "Min and max chunk sizes should be greater than 0, and max >= min"
    }
    val buffer = ArrayList<T>(max)
    collect {
        buffer.add(it)
        if(buffer.size >= max) {
            send(buffer.toList())
            buffer.clear()
        } else if (min <= buffer.size) {
            val offered = offer(buffer.toList())
            if (offered) {
                buffer.clear()
            }
        }
    }
    if (buffer.size > 0) send(buffer.toList())
}.buffer(1)
