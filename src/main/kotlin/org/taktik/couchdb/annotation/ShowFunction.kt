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

package org.taktik.couchdb.annotation

/**
 * Annotation for defining show functions embedded in repositories.
 * @author henrik lundgren
 */
@Target(AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY_GETTER, AnnotationTarget.PROPERTY_SETTER, AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class ShowFunction(
        /**
         * The name of the show function.
         * @return
         */
        val name: String,
        /**
         * Inline show function.
         * @return
         */
        val function: String = "",
        /**
         * Show functions are best stored in a separate files.
         *
         * By specifying the file parameter a function can be loaded from the classpath.
         * The path is relative to the class annotated by this annotation.
         *
         * If the file my_show_func.json is in the same directory as the repository this
         * parameter should be set to "my_show_func.js".
         *
         * @return
         */
        val file: String = "")
