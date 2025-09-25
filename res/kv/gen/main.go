package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	const (
		getFrequency      = 5
		notFoundFrequency = 5
		updateFrequency   = 2
		lines             = 100_000
	)

	state := make(map[int]int)
	sb := strings.Builder{}
	for i := 0; i < lines; i++ {
		get := random(getFrequency)
		if get {
			if random(notFoundFrequency) {
				sb.WriteString(formatNotFound(unknownKey(state)))
			} else {
				if len(state) == 0 {
					sb.WriteString(formatNotFound(unknownKey(state)))
				} else {
					k := existingKey(state)
					v := state[k]
					sb.WriteString(formatGet(k, v))
				}
			}
		} else {
			if random(updateFrequency) {
				k := existingKey(state)
				v := rand.Int()
				sb.WriteString(formatPut(k, v))
				state[k] = v
			} else {
				k := unknownKey(state)
				v := rand.Int()
				sb.WriteString(formatPut(k, v))
				state[k] = v
			}
		}
	}
	return os.WriteFile("input1.txt", []byte(sb.String()), 0644)
}

func formatGet(key, value int) string {
	return fmt.Sprintf("GET %d %d\n", key, value)
}

func formatNotFound(key int) string {
	return fmt.Sprintf("GET %d NOT_FOUND\n", key)
}

func formatPut(key, value int) string {
	return fmt.Sprintf("PUT %d %d\n", key, value)
}

func existingKey(state map[int]int) int {
	keys := make([]int, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}

	return keys[rand.Intn(len(keys))]
}

func unknownKey(state map[int]int) int {
	for {
		i := rand.Int()
		if _, contains := state[i]; !contains {
			return i
		}
	}
}

func random(max int) bool {
	return rand.Intn(max) == 0
}
