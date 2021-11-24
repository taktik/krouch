package org.taktik.couchdb.handlers

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

class ZonedDateTimeSerializer : JsonSerializer<ZonedDateTime?>() {
    override fun serialize(value: ZonedDateTime?, gen: JsonGenerator, serializers: SerializerProvider?) = value?.let {
        gen.writeString(it.format(DateTimeFormatter.ISO_ZONED_DATE_TIME))
    } ?: gen.writeNull()
}
