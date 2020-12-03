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

/**
 *
 * @author henrik lundgren
 */
class Assert {

    companion object {

        @JvmOverloads
        fun notNull(o: Any?, message: String? = null) {
            if (o == null) {
                throw message?.let { NullPointerException(it) } ?: NullPointerException()
            }
        }

        fun isNull(o: Any?, message: String) {
            if (o != null) {
                throwIllegalArgument(message)
            }
        }

        fun isTrue(b: Boolean) {
            isTrue(b, null)
        }

        fun isTrue(b: Boolean, message: String?) {
            if (!b) {
                throwIllegalArgument(message)
            }
        }

        fun notEmpty(c: Collection<*>, message: String) {
            notNull(c, message)
            if (c.isEmpty()) {
                throwIllegalArgument(message)
            }
        }

        fun notEmpty(a: Array<Any?>, message: String) {
            notNull(a, message)
            if (a.isEmpty()) {
                throwIllegalArgument(message)
            }
        }

        @JvmOverloads
        fun hasText(s: String?, message: String? = null) {
            if (s.isNullOrEmpty()) {
                throwIllegalArgument(message)
            }
        }

        private fun throwIllegalArgument(s: String?) {
            throw s?.let { IllegalArgumentException(it) } ?: IllegalArgumentException()
        }
    }
}
