package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf(err.Error())
	}
}

func run() error {
	file, err := os.Open("private/emails.csv")
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	scanner := bufio.NewScanner(file)
	// Omit header
	scanner.Scan()
	domains := make(map[string]int)
	for scanner.Scan() {
		line := scanner.Text()
		email := strings.Split(line, ",")[0]
		domain := strings.ToLower(email[strings.Index(email, "@")+1:])
		domains[domain]++
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	keys := make([]string, 0, len(domains))
	for key := range domains {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if domains[keys[i]] > domains[keys[j]] {
			return true
		}
		if domains[keys[i]] < domains[keys[j]] {
			return false
		}
		return keys[i] < keys[j]
	})
	for _, key := range keys {
		fmt.Printf("%s: %d\n", key, domains[key])
	}

	return nil
}
