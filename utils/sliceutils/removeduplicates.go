package sliceutils

// RemoveDuplicates removes any duplicate entries from a list.
func RemoveDuplicates[T comparable](in []T) []T {
	// TODO(brett19): Improve the performance of RemoveDuplicates...

	dupMap := make(map[T]bool)
	var out []T
	for _, v := range in {
		if _, ok := dupMap[v]; !ok {
			dupMap[v] = true
			out = append(out, v)
		}
	}
	return out
}
