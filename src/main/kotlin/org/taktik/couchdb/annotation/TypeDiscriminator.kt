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
 * Used to distinguish a type's documents in the database.
 *
 * Declare on fields or getter methods in order for them to be used in generated views filter conditions.
 *
 * Declare on type in order specify a custom filter condition.
 *
 * A TypeDiscriminator declared on type level cannot be mixed with TypeDiscriminators declared onb fields.
 * @author henrik lundgren
 */
@Target(AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY_GETTER, AnnotationTarget.PROPERTY_SETTER, AnnotationTarget.FIELD, AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class TypeDiscriminator(
        /**
         * If TypeDiscriminator is declared on type level, a filter condition must be specified.
         * This condition is inserted along other conditions in the generated views map function:
         * function(doc) { if(CONDITION INSERTED HERE && doc.otherField) {emit(null, doc._id)} }
         *
         * Not valid to use if declared on field or method level.
         */
        val value: String = "")
