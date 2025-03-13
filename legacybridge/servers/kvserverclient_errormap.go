/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package servers

import (
	"encoding/binary"

	"github.com/couchbase/gocbcore/v10/memd"
)

func (c *KvServerClient) handleCmdGetErrorMapReq(pak *memd.Packet) {
	if !c.validatePacket(pak, ValidateFlagAllowValue) {
		return
	}

	if len(pak.Value) != 2 {
		c.sendInvalidArgs(pak, "value should have a length of 2")
		return
	}

	mapVer := binary.BigEndian.Uint16(pak.Value[0:])

	// TODO(brett19): Support sending error map v1 to clients...
	if mapVer != 2 {
		c.sendInvalidArgs(pak, "error map version must be '2'")
		return
	}

	c.sendSuccessReply(pak, nil, []byte(ErrorMapData), nil)
}

var ErrorMapData string = `{
	"errors": {
	  "0": {
		"attrs": [
		  "success"
		],
		"desc": "Success",
		"name": "SUCCESS"
	  },
	  "1": {
		"attrs": [
		  "item-only"
		],
		"desc": "Not Found",
		"name": "KEY_ENOENT"
	  },
	  "2": {
		"attrs": [
		  "item-only"
		],
		"desc": "key already exists, or CAS mismatch",
		"name": "KEY_EEXISTS"
	  },
	  "3": {
		"attrs": [
		  "item-only",
		  "invalid-input"
		],
		"desc": "Value is too big",
		"name": "E2BIG"
	  },
	  "4": {
		"attrs": [
		  "internal",
		  "invalid-input"
		],
		"desc": "Invalid packet",
		"name": "EINVAL"
	  },
	  "5": {
		"attrs": [
		  "internal",
		  "item-only"
		],
		"desc": "Not Stored",
		"name": "NOT_STORED"
	  },
	  "6": {
		"attrs": [
		  "item-only",
		  "invalid-input"
		],
		"desc": "Existing document not a number",
		"name": "DELTA_BADVAL"
	  },
	  "7": {
		"attrs": [
		  "fetch-config",
		  "invalid-input"
		],
		"desc": "Server does not know about this vBucket",
		"name": "NOT_MY_VBUCKET"
	  },
	  "8": {
		"attrs": [
		  "conn-state-invalidated"
		],
		"desc": "Not connected to any bucket",
		"name": "NO_BUCKET"
	  },
	  "9": {
		"attrs": [
		  "item-locked",
		  "item-only",
		  "retry-now"
		],
		"desc": "Requested resource is locked",
		"name": "LOCKED"
	  },
	  "20": {
		"attrs": [
		  "conn-state-invalidated",
		  "auth"
		],
		"desc": "Authentication failed",
		"name": "AUTH_ERROR"
	  },
	  "21": {
		"attrs": [
		  "special-handling"
		],
		"desc": "Continue authentication processs",
		"name": "AUTH_CONTINUE"
	  },
	  "22": {
		"attrs": [
		  "invalid-input"
		],
		"desc": "Invalid range requested",
		"name": "ERANGE"
	  },
	  "23": {
		"attrs": [
		  "dcp",
		  "special-handling"
		],
		"desc": "Rollback",
		"name": "ROLLBACK"
	  },
	  "24": {
		"attrs": [
		  "support"
		],
		"desc": "Not authorized for command",
		"name": "EACCESS"
	  },
	  "25": {
		"attrs": [
		  "conn-state-invalidated"
		],
		"desc": "Server not initialized",
		"name": "NOT_INITIALIZED"
	  },
	  "30": {
		"attrs": [
		  "temp",
		  "retry-later",
		  "rate-limit"
		],
		"desc": "Rate limited: Network Ingress",
		"name": "RATE_LIMITED_NETWORK_INGRESS"
	  },
	  "31": {
		"attrs": [
		  "temp",
		  "retry-later",
		  "rate-limit"
		],
		"desc": "Rate limited: Network Egress",
		"name": "RATE_LIMITED_NETWORK_EGRESS"
	  },
	  "32": {
		"attrs": [
		  "conn-state-invalidated",
		  "rate-limit"
		],
		"desc": "Rate limited: Max Connections",
		"name": "RATE_LIMITED_MAX_CONNECTIONS"
	  },
	  "33": {
		"attrs": [
		  "temp",
		  "retry-later",
		  "rate-limit"
		],
		"desc": "Rate limited: Max Commands",
		"name": "RATE_LIMITED_MAX_COMMANDS"
	  },
	  "80": {
		"attrs": [
		  "support"
		],
		"desc": "Unknown frame info identifier encountered. Maybe a newer server version knows about it",
		"name": "UNKNOWN_FRAME_INFO"
	  },
	  "81": {
		"attrs": [
		  "support"
		],
		"desc": "Unknown command. Maybe a newer server version knows about it",
		"name": "UNKNOWN_COMMAND"
	  },
	  "82": {
		"attrs": [
		  "temp",
		  "retry-later"
		],
		"desc": "No memory available to store item. Add memory or remove some items and try later",
		"name": "ENOMEM"
	  },
	  "83": {
		"attrs": [
		  "support"
		],
		"desc": "Command not supported with current bucket type/configuration",
		"name": "NOT_SUPPORTED"
	  },
	  "84": {
		"attrs": [
		  "internal",
		  "conn-state-invalidated"
		],
		"desc": "Internal error. Reconnect recommended",
		"name": "EINTERNAL"
	  },
	  "85": {
		"attrs": [
		  "temp",
		  "retry-now"
		],
		"desc": "Busy, try again",
		"name": "EBUSY"
	  },
	  "86": {
		"attrs": [
		  "temp",
		  "retry-now"
		],
		"desc": "Temporary failure. Try again",
		"name": "ETMPFAIL"
	  },
	  "87": {
		"attrs": [
		  "invalid-input"
		],
		"desc": "Invalid extended attribute",
		"name": "XATTR_EINVAL"
	  },
	  "88": {
		"attrs": [
		  "invalid-input"
		],
		"desc": "Operation specified an unknown collection.",
		"name": "UNKNOWN_COLLECTION"
	  },
	  "89": {
		"attrs": [
		  "retry-later"
		],
		"desc": "No collections manifest has been set.",
		"name": "NO_COLLECTIONS_MANIFEST"
	  },
	  "1f": {
		"attrs": [
		  "conn-state-invalidated",
		  "auth"
		],
		"desc": "Reauthentication required",
		"name": "AUTH_STALE"
	  },
	  "8a": {
		"attrs": [
		  "invalid-input"
		],
		"desc": "The manifest cannot applied to the bucket's vbuckets.",
		"name": "CANNOT_APPLY_COLLECTIONS_MANIFEST"
	  },
	  "8c": {
		"attrs": [
		  "invalid-input"
		],
		"desc": "Operation specified an unknown scope.",
		"name": "UNKNOWN_SCOPE"
	  },
	  "8d": {
		"attrs": [
		  "invalid-input"
		],
		"desc": "Operations stream-ID usage is incorrect.",
		"name": "DCP stream-ID invalid"
	  },
	  "a": {
		"attrs": [
		  "conn-state-invalidated"
		],
		"desc": "Stream not found",
		"name": "STREAM_NOT_FOUND"
	  },
	  "a0": {
		"attrs": [
		  "invalid-input"
		],
		"desc": "Durability level is invalid",
		"name": "DurabilityInvalidLevel"
	  },
	  "a1": {
		"attrs": [
		  "item-only",
		  "invalid-input"
		],
		"desc": "Durability requirements are impossible to achieve",
		"name": "DurabilityImpossible"
	  },
	  "a2": {
		"attrs": [
		  "item-only",
		  "retry-later"
		],
		"desc": "The requested key has a pending synchronous write",
		"name": "SyncWriteInProgress"
	  },
	  "a3": {
		"attrs": [
		  "item-only"
		],
		"desc": "The SyncWrite request has not completed in the specified time and has ambiguous result - it may Succeed or Fail; but the final value is not yet known",
		"name": "SyncWriteAmbiguous"
	  },
	  "a4": {
		"attrs": [
		  "item-only",
		  "retry-later"
		],
		"desc": "The requested key has a SyncWrite which is being re-committed.",
		"name": "SyncWriteReCommitInProgress"
	  },
	  "b": {
		"attrs": [
		  "conn-state-invalidated"
		],
		"desc": "Opaque does not match",
		"name": "OPAQUE_NO_MATCH"
	  },
	  "c0": {
		"attrs": [
		  "subdoc",
		  "item-only"
		],
		"desc": "Subdoc: Path not found in document",
		"name": "SUBDOC_PATH_ENOENT"
	  },
	  "c1": {
		"attrs": [
		  "subdoc",
		  "item-only"
		],
		"desc": "Subdoc: Path and document disagree on structure",
		"name": "SUBDOC_PATH_MISMATCH"
	  },
	  "c2": {
		"attrs": [
		  "subdoc",
		  "invalid-input"
		],
		"desc": "Subdoc: Invalid path (bad syntax or unacceptable semantics for command",
		"name": "SUBDOC_PATH_EINVAL"
	  },
	  "c3": {
		"attrs": [
		  "subdoc",
		  "invalid-input"
		],
		"desc": "Subdoc: Path size exceeds limit",
		"name": "SUBDOC_PATH_E2BIG"
	  },
	  "c4": {
		"attrs": [
		  "subdoc",
		  "invalid-input"
		],
		"desc": "Subdoc: Path is too deep to be parsed",
		"name": "SUBDOC_PATH_E2DEEP"
	  },
	  "c5": {
		"attrs": [
		  "subdoc",
		  "invalid-input"
		],
		"desc": "Subdoc: Value invalid for insertion",
		"name": "SUBDOC_VALUE_CANTINSERT"
	  },
	  "c6": {
		"attrs": [
		  "subdoc",
		  "item-only"
		],
		"desc": "Subdoc: Document not JSON",
		"name": "SUBDOC_DOC_NOTJSON"
	  },
	  "c7": {
		"attrs": [
		  "subdoc",
		  "item-only"
		],
		"desc": "Subdoc: Existing numeric value is not within range",
		"name": "SUBDOC_NUM_ERANGE"
	  },
	  "c8": {
		"attrs": [
		  "subdoc",
		  "item-only"
		],
		"desc": "Subdoc: Invalid value passed for delta (out of range, or not an integer",
		"name": "SUBDOC_DELTA_EINVAL"
	  },
	  "c9": {
		"attrs": [
		  "subdoc",
		  "item-only"
		],
		"desc": "Subdoc: Path already exists",
		"name": "SUBDOC_PATH_EEXISTS"
	  },
	  "ca": {
		"attrs": [
		  "subdoc",
		  "invalid-input",
		  "item-only"
		],
		"desc": "Subdoc: Value is too deep, or would make the document too deep",
		"name": "SUBDOC_VALUE_ETOODEEP"
	  },
	  "cb": {
		"attrs": [
		  "subdoc",
		  "invalid-input"
		],
		"desc": "Subdoc: Lookup and mutation commands found within single packet",
		"name": "SUBDOC_INVALID_COMBO"
	  },
	  "cc": {
		"attrs": [
		  "subdoc",
		  "special-handling"
		],
		"desc": "Subdoc: Some (or all) commands failed. Inspect payload for details",
		"name": "SUBDOC_MULTI_PATH_FAILURE"
	  },
	  "cd": {
		"attrs": [
		  "item-deleted",
		  "success",
		  "subdoc"
		],
		"desc": "Subdoc: Success, but the affected document was (and still is) deleted",
		"name": "SUBDOC_SUCCESS_DELETED"
	  },
	  "ce": {
		"attrs": [
		  "subdoc",
		  "invalid-input"
		],
		"desc": "Subdoc: The flag combination doesn't make any sense",
		"name": "SUBDOC_XATTR_INVALID_FLAG_COMBO"
	  },
	  "cf": {
		"attrs": [
		  "subdoc",
		  "invalid-input"
		],
		"desc": "Subdoc: The key combination of the xattrs is not allowed",
		"name": "SUBDOC_XATTR_INVALID_KEY_COMBO"
	  },
	  "d0": {
		"attrs": [
		  "subdoc",
		  "invalid-input"
		],
		"desc": "Subdoc: The server don't know about the specified macro",
		"name": "SUBDOC_XATTR_UNKNOWN_MACRO"
	  },
	  "d1": {
		"attrs": [
		  "subdoc",
		  "invalid-input"
		],
		"desc": "Subdoc: The server don't know about the specified virtual attribute",
		"name": "SUBDOC_XATTR_UNKNOWN_VATTR"
	  },
	  "d2": {
		"attrs": [
		  "subdoc",
		  "invalid-input"
		],
		"desc": "Subdoc: Can't modify virtual attributes",
		"name": "SUBDOC_XATTR_CANT_MODIFY_VATTR"
	  },
	  "d3": {
		"attrs": [
		  "item-deleted",
		  "subdoc",
		  "special-handling"
		],
		"desc": "Subdoc: One or more paths in a multi-path command failed on a deleted document",
		"name": "SUBDOC_MULTI_PATH_FAILURE_DELETED"
	  },
	  "d4": {
		"attrs": [
		  "subdoc",
		  "invalid-input"
		],
		"desc": "Subdoc: Invalid XATTR order (xattrs should come first)",
		"name": "SUBDOC_INVALID_XATTR_ORDER"
	  },
	  "d5": {
		"attrs": [
		  "subdoc",
		  "invalid-input"
		],
		"desc": "Subdoc: The server don't know about (or support) the specified virtual macro",
		"name": "SUBDOC_XATTR_UNKNOWN_VATTR_MACRO"
	  },
	  "d6": {
		"attrs": [
		  "subdoc",
		  "item-only"
		],
		"desc": "Subdoc: Only deleted documents can be revived",
		"name": "SUBDOC_CAN_ONLY_REVIVE_DELETED_DOCUMENTS"
	  },
	  "d7": {
		"attrs": [
		  "subdoc",
		  "item-only"
		],
		"desc": "Subdoc: A deleted document can't have a value",
		"name": "SUBDOC_DELETED_DOCUMENT_CANT_HAVE_VALUE"
	  }
	},
	"revision": 1,
	"version": 2
  }`
