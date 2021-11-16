package org.taktik.couchdb.handlers

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import java.time.ZonedDateTime

class ZonedDateTimeDeserializer : JsonDeserializer<ZonedDateTime>() {
    override fun deserialize(
            p: JsonParser?,
            ctxt: DeserializationContext?
    ): ZonedDateTime = ZonedDateTime.parse(p?.valueAsString)
}
