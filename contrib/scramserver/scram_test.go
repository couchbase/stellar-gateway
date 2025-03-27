/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package scramserver

import (
	"testing"
)

func TestScram(t *testing.T) {
	srvr, err := newScramServerWithSaltAndNonce("SCRAM-SHA1", "saltSALTsalt", "serverNONCE")
	if err != nil {
		t.Fatalf("Failed to create scram auth: %v", err)
	}

	err = srvr.SetPassword([]byte("pencil"))
	if err != nil {
		t.Fatalf("Failed to set password: %v", err)
	}

	u, err := srvr.Start([]byte("n,,n=user,r=clientNONCE"))
	if err != nil {
		t.Fatalf("Failed to start scram auth: %v", err)
	}

	if u != "user" {
		t.Fatalf("Username should have been user but was: %s", u)
	}

	out := srvr.Out()
	if string(out) != "r=clientNONCEserverNONCE,s=c2FsdFNBTFRzYWx0,i=4096" {
		t.Fatalf("Output from start should have been r=clientNONCEserverNONCE,s=c2FsdFNBTFRzYWx0,i=4096 "+
			"was: %s", string(out))
	}

	err = srvr.Step1([]byte("c=biws,r=clientNONCEserverNONCE,p=I4oktcY7BOL0Agn0NlWRXlRP1mg="))
	if err != nil {
		t.Fatalf("Failed to step scram auth: %v", err)
	}

	out = srvr.Out()
	if string(out) != "v=oKPvB1bE/9ydptJ+kohMgL+NdM0=" {
		t.Fatalf("Output from start should have been v=oKPvB1bE/9ydptJ+kohMgL+NdM0= "+
			"was: %s", string(out))
	}
}
