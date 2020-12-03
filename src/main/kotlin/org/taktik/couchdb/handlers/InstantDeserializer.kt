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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import java.time.Instant
import java.math.BigDecimal

class InstantDeserializer : JsonDeserializer<Instant>() {
    override fun deserialize(jp: JsonParser, ctxt: DeserializationContext): Instant = getInstant(jp.decimalValue)

    private fun getInstant(value: BigDecimal): Instant =
            Instant.ofEpochSecond(value.divide(BD_1000).toLong(), value.remainder(BD_1000).multiply(BD_1000000).toLong())

    companion object {
        private val BD_1000000 = BigDecimal.valueOf(1000000)
        private val BD_1000 = BigDecimal.valueOf(1000)
    }
}
