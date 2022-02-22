/*
 * Copyright (c) 2020. Taktik SA, All rights reserved.
 */
package org.taktik.couchdb

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.github.pozo.KotlinBuilder
import org.taktik.couchdb.entity.Attachment
import org.taktik.couchdb.entity.Versionable
import java.io.Serializable

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@KotlinBuilder

/**
 * A User
 * This entity is a root level object. It represents an user that can log in to the iCure platform. It is serialized in JSON and saved in the underlying icure-base CouchDB database.
 * A User conforms to a series of interfaces:
 * - StoredDocument
 * - Principal
 *
 * @property id the Id of the user. We encourage using either a v4 UUID or a HL7 Id.
 * @property rev the revision of the user in the database, used for conflict management / optimistic locking.
 * @property created the timestamp (unix epoch in ms) of creation of the user, will be filled automatically if missing. Not enforced by the application server.
 * @property deletionDate hard delete (unix epoch in ms) timestamp of the object. Filled automatically when user is deleted.
 * @property name Last name of the user. This is the official last name that should be used for official administrative purposes.
 * @property properties Extra properties for the user. Those properties are typed (see class Property)
 * @property roles Roles specified for the user
 * @property permissions If permission to modify patient data is granted or revoked
 * @property type Authorization source for user. 'Database', 'ldap' or 'token'
 * @property status State of user's activeness: 'Active', 'Disabled' or 'Registering'
 * @property login Username for this user. We encourage using an email address
 * @property passwordHash Hashed version of the password (BCrypt is used for hashing)
 * @property secret Secret token used to verify 2fa
 * @property use2fa Whether the user has activated two factors authentication
 * @property groupId id of the group (practice/hospital) the user is member of
 * @property healthcarePartyId Id of the healthcare party if the user is a healthcare party.
 * @property patientId Id of the patient if the user is a patient
 * @property autoDelegations Delegations that are automatically generated client side when a new database object is created by this user
 * @property createdDate the timestamp (unix epoch in ms) of creation of the user, will be filled automatically if missing. Not enforced by the application server.
 * @property lastLoginDate the timestamp (unix epoch in ms) of last login of the user.
 * @property termsOfUseDate the timestamp (unix epoch in ms) of the latest validation of the terms of use of the application
 * @property email email address of the user.
 * @property applicationTokens Deprecated : Use authenticationTokens instead - Long lived authentication tokens used for inter-applications authentication
 * @property authenticationTokens Encrypted and time-limited Authentication tokens used for inter-applications authentication
 */

data class User(
        @JsonProperty("_id") override val id: String,
        @JsonProperty("_rev") override val rev: String? = null,
        @JsonProperty("deleted") val deletionDate: Long? = null,
        val created: Long? = null,

        val name: String? = null,
        val roles: Set<String> = emptySet(),
        val login: String? = null,
        val passwordHash: String? = null,
        val secret: String? = null,
        @JsonProperty("isUse2fa") val use2fa: Boolean? = null,
        val groupId: String? = null,
        val healthcarePartyId: String? = null,
        val patientId: String? = null,
        val deviceId: String? = null,
        val createdDate: Long? = null,
        val lastLoginDate: Long? = null,
        val expirationDate: Long? = null,
        val termsOfUseDate: Long? = null,

        val email: String? = null,
        val mobilePhone: String? = null,

        @Deprecated("Application tokens stocked in clear and eternal. Replaced by authenticationTokens")
        val applicationTokens: Map<String, String>? = null,

        @JsonProperty("_attachments") val attachments: Map<String, Attachment>? = emptyMap(),
        @JsonProperty("_conflicts") val conflicts: List<String>? = emptyList(),
        @JsonProperty("rev_history") override val revHistory : Map<String, String>? = emptyMap(),

) : CouchDbDocument, Cloneable, Serializable {
        override fun withIdRev(id: String?, rev: String) = if (id != null) this.copy(id = id, rev = rev) else this.copy(rev = rev)
}
