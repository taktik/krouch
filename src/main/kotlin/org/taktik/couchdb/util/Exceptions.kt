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

package org.taktik.couchdb.util

import org.taktik.couchdb.exception.DbAccessException

/**
 *
 * @author Henrik Lundgren
 * created 1 nov 2009
 *
 */

class Exceptions {

    companion object {

        fun propagate(e: Throwable): RuntimeException {
            return when (e) {
                is RuntimeException -> e
                else -> DbAccessException(e)
            }
        }

        fun newRTE(format: String, vararg args: Any): RuntimeException {
            return RuntimeException(String.format(format, *args))
        }
    }
}
