package channelmerge

type Merged[A any, B any] struct {
	A A
	B B
}

func Merge[A any, B any](a <-chan A, b <-chan B) <-chan Merged[A, B] {
	outputCh := make(chan Merged[A, B])

	go func() {
		// read the first value from a
		currentA, ok := <-a
		if !ok {
			// if the input channel closes before the first value, we
			// close the output channel and stop reading...
			close(outputCh)
			return
		}

		// read the first value from b
		currentB, ok := <-b
		if !ok {
			// if the input channel closes before the first value, we
			// close the output channel and stop reading...
			close(outputCh)
			return
		}

		// once we have the first values for a and b, output that.
		outputCh <- Merged[A, B]{
			A: currentA,
			B: currentB,
		}

	MainLoop:
		for {
			select {
			case newA, ok := <-a:
				if !ok {
					break MainLoop
				}
				currentA = newA
			case newB, ok := <-b:
				if !ok {
					break MainLoop
				}
				currentB = newB
			}

			outputCh <- Merged[A, B]{
				A: currentA,
				B: currentB,
			}
		}

		close(outputCh)
	}()

	return outputCh
}
