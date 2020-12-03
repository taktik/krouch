/*
 *  iCure Data Stack. Copyright (c) 2020 Taktik SA
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as
 *     published by the Free Software Foundation, either version 3 of the
 *     License, or (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful, but
 *     WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *     Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public
 *     License along with this program.  If not, see
 *     <https://www.gnu.org/licenses/>.
 */
package org.taktik.couchdb.handlers

import com.fasterxml.jackson.databind.JsonSerializer
import java.time.Instant
import kotlin.Throws
import java.io.IOException
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import java.math.BigDecimal

class InstantSerializer : JsonSerializer<Instant?>() {
    override fun serialize(value: Instant?, jgen: JsonGenerator, provider: SerializerProvider) = value?.let {
        jgen.writeNumber(getBigDecimal(it))
    } ?: jgen.writeNull()

    private fun getBigDecimal(value: Instant) =
            BigDecimal.valueOf(1000L * value.epochSecond).add(BigDecimal.valueOf(value.nano.toLong()).divide(BD_1000000))

    override fun isEmpty(value: Instant?): Boolean {
        return value == null
    }

    companion object {
        private val BD_1000000 = BigDecimal.valueOf(1000000)
    }
}
