package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	idxCountry  = 12
	idxLastOpen = 19
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
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	//statsTime(lines, "US")
	statsCountry(lines)

	return nil
}

func statsTime(lines []string, countryFilter string) {
	hours := make([]int, 24)
	for _, line := range lines {
		split := strings.Split(line, ",")

		lastOpenStr := split[idxLastOpen]
		country := split[12]

		if lastOpenStr != "" && country == countryFilter {
			lastOpen := mustParse(lastOpenStr)
			hours[lastOpen.Hour()]++
		}
	}

	fmt.Println("UTC time:")
	for hour, count := range hours {
		fmt.Printf("%d: %d\n", hour, count)
	}
}

func statsCountry(lines []string) {
	countries := make(map[string]int)
	for _, line := range lines {
		split := strings.Split(line, ",")
		country := split[idxCountry]
		countries[country]++
	}

	keys := make([]string, 0, len(countries))
	for key := range countries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return countries[keys[i]] > countries[keys[j]]
	})

	fmt.Println("Countries:")
	for _, key := range keys {
		fmt.Printf("%s: %d\n", key, countries[key])
	}
}

func mustParse(dateString string) time.Time {
	layout := "2006-01-02T15:04:05.000Z"
	t, err := time.Parse(layout, dateString)
	if err != nil {
		log.Fatal(err.Error())
	}
	return t
}
