package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

const (
	getFrequency      = 5
	deleteFrequency   = 10
	notFoundFrequency = 5
	updateFrequency   = 2
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
	if err := run2(lines); err != nil {
		panic(err)
	}
}

func run1(lines int) error {
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
				if len(state) == 0 {
					i--
					continue
				}
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

func run2(lines int) error {
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
	}
	return os.WriteFile("input2.txt", []byte(sb.String()), 0644)
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

func formatDelete(key int) string {
	return fmt.Sprintf("DELETE %d\n", key)
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
