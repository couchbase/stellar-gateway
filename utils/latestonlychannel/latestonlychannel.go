/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package latestonlychannel

// LatestOnlyChannel creates a channel pipe which guarentees that the input channel
// will never block by having no queue and discarding older entries from being sent
// to the output once newer values are received on the input channel.
// You must close the input channel to release internal resources.
func Wrap[T any](inputCh <-chan T) <-chan T {
	outputCh := make(chan T)

	go func() {
		// we have MainLoop which loops around doing send/receives with some
		// specialized logic in the send handler which loops around constantly
		// receiving any new updates on the input channel and then updating
		// what we are trying to write out to the output channel.
	MainLoop:
		for {
			latestData, ok := <-inputCh
			if !ok {
				// input channel closed
				break MainLoop
			}

		SendLoop:
			for {
				select {
				case outputCh <- latestData:
					// once we've successfully sent the latest update, we
					// go back to the top and read some data and try to send
					// it.  These concepts are split apart to ensure that
					// we don't spam the output channel with any more updates
					// than actually are received at the input channel.
					// Eg: We guarentee count(outputCh) <= count(inputCh)
					break SendLoop
				case updatedData, ok := <-inputCh:
					if !ok {
						break MainLoop
					}

					latestData = updatedData
				}
			}
		}

		close(outputCh)
	}()

	return outputCh
}
