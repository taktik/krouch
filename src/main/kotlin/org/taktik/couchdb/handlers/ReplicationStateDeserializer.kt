package org.taktik.couchdb.handlers

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import org.taktik.couchdb.entity.Scheduler

class ReplicationStateDeserializer : JsonDeserializer<Scheduler.ReplicationState>() {
    override fun deserialize(
            p: JsonParser?,
            ctxt: DeserializationContext?
    ): Scheduler.ReplicationState = Scheduler.ReplicationState.fromString(p?.valueAsString)
}
