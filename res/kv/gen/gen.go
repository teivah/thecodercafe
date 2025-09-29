package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
)

const (
	getFrequency      = 5
	deleteFrequency   = 10
	notFoundFrequency = 5
	updateFrequency   = 2
	keyLength         = 5
	maxValue          = 1_000
)

func main() {
	prog := os.Args[0]
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: %s (input1|input2) <lines>\n", prog)
	}

	flag.Parse()
	args := flag.Args()

	if len(args) != 2 {
		flag.Usage()
		os.Exit(1)
	}

	lines, err := strconv.Atoi(args[1])
	if err != nil {
		flag.Usage()
		os.Exit(1)
	}

	mode := args[0]
	switch mode {
	case "input1":
		if err := run1(lines); err != nil {
			panic(err)
		}
	case "input2":
		if err := run2(lines); err != nil {
			panic(err)
		}
	default:
		flag.Usage()
		os.Exit(1)
	}
}

func run1(lines int) error {
	state := make(map[string]string)
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
				if len(state) == 0 {
					i--
					continue
				}
				k := existingKey(state)
				v := randomValue(maxValue)
				sb.WriteString(formatPut(k, v))
				state[k] = v
			} else {
				k := unknownKey(state)
				v := randomValue(maxValue)
				sb.WriteString(formatPut(k, v))
				state[k] = v
			}
		}
	}
	return os.WriteFile("input1.txt", []byte(sb.String()), 0644)
}

func run2(lines int) error {
	state := make(map[string]string)
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
			if random(deleteFrequency) {
				if len(state) == 0 {
					sb.WriteString(formatNotFound(unknownKey(state)))
				} else {
					k := existingKey(state)
					sb.WriteString(formatDelete(k))
					delete(state, k)
				}
			} else {
				if random(updateFrequency) {
					if len(state) == 0 {
						i--
						continue
					}
					k := existingKey(state)
					v := randomValue(maxValue)
					sb.WriteString(formatPut(k, v))
					state[k] = v
				} else {
					k := unknownKey(state)
					v := randomValue(maxValue)
					sb.WriteString(formatPut(k, v))
					state[k] = v
				}
			}
		}
	}
	return os.WriteFile("input2.txt", []byte(sb.String()), 0644)
}

func formatGet(key, value string) string {
	return fmt.Sprintf("GET %s %s\n", key, value)
}

func formatNotFound(key string) string {
	return fmt.Sprintf("GET %s NOT_FOUND\n", key)
}

func formatPut(key, value string) string {
	return fmt.Sprintf("PUT %s %s\n", key, value)
}

func formatDelete(key string) string {
	return fmt.Sprintf("DELETE %s\n", key)
}

func randomKey() string {
	b := make([]byte, keyLength)
	for i := range b {
		b[i] = byte('a' + rand.IntN(26)) // v2 uses IntN
	}
	return string(b)
}

func randomValue(max int) string {
	return strconv.Itoa(rand.IntN(max))
}

func existingKey(state map[string]string) string {
	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}

	return keys[rand.IntN(len(keys))]
}

func unknownKey(state map[string]string) string {
	for {
		k := randomKey()
		if _, contains := state[k]; !contains {
			return k
		}
	}
}

func random(max int) bool {
	return rand.IntN(max) == 0
}
