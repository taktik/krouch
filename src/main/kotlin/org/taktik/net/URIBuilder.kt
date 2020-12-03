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

package org.taktik.net

import org.apache.http.client.utils.URIBuilder
import org.apache.http.message.BasicNameValuePair

fun java.net.URI.append(s: String?): java.net.URI {
    return s?.let { s -> URIBuilder(this).let { it.setPathSegments(it.pathSegments + s.trim('/').split("/")) }.build() } ?: this
}

fun java.net.URI.param(k: String, v: String): java.net.URI {
    return URIBuilder(this).setParameter(k, v).build()
}

fun java.net.URI.params(map: Map<String, String>): java.net.URI {
    return URIBuilder(this).setParameters(map.entries.map { (k, v) -> BasicNameValuePair(k, v) }.toList()).build()
}
