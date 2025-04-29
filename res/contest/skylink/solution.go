package main

import (
	_ "embed"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

//go:embed input.log
var input string

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	g, source, sinks, err := parse()
	if err != nil {
		return err
	}

	res := 0
	for _, sink := range sinks {
		for {
			path := getAugmentingPaths(g, source, sink)
			if len(path) == 0 {
				break
			}

			bottleneck := getBottleneck(g, path)
			fmt.Println(path, bottleneck)
			for _, edge := range path {
				g[edge.from][edge.to] -= bottleneck
				g[edge.to][edge.from] += bottleneck
			}
			res += bottleneck
		}
	}

	fmt.Println(res)
	return nil
}

func getAugmentingPaths(g map[string]map[string]int, from, to string) []edge {
	type state struct {
		current string
		edges   []edge
	}

	q := []state{{current: from}}
	visited := make(map[string]bool)
	for len(q) != 0 {
		s := q[0]
		q = q[1:]

		if s.current == to {
			return s.edges
		}

		if visited[s.current] {
			continue
		}
		visited[s.current] = true

		for to, quota := range g[s.current] {
			if quota == 0 {
				continue
			}
			e := edge{s.current, to}
			q = append(q, state{
				current: to,
				edges:   append(slices.Clone(s.edges), e),
			})
		}
	}

	return nil
}

func getBottleneck(g map[string]map[string]int, edges []edge) int {
	res := math.MaxInt
	for _, edge := range edges {
		res = min(res, g[edge.from][edge.to])
	}
	if res == math.MaxInt {
		panic("invalid state")
	}
	return res
}

type edge struct {
	from string
	to   string
}

func parse() (map[string]map[string]int, string, []string, error) {
	reTransmission := regexp.MustCompile(`NODE (A\d+) RELAYS (A\d+) UNDER QUOTA (\d+)`)
	reAlert := regexp.MustCompile(`ALERT: PRIMARY NODE IS (A\d+)`)
	reCritical := regexp.MustCompile(`CRITICAL: FINAL ARRIVAL POINTS ARE (A\d+(?:, A\d+)*)`)

	lines := strings.Split(strings.TrimSpace(input), "\n")
	g := make(map[string]map[string]int)
	var source string
	var sinks []string

	for _, line := range lines {
		if matches := reTransmission.FindStringSubmatch(line); len(matches) == 4 {
			source := matches[1]
			destination := matches[2]
			s := matches[3]
			capacity, err := strconv.Atoi(s)
			if err != nil {
				return nil, "", nil, err
			}

			m, ok := g[source]
			if !ok {
				m = make(map[string]int)
				g[source] = m
			}
			m[destination] = capacity

			m, ok = g[destination]
			if !ok {
				m = make(map[string]int)
				g[destination] = m
			}
			m[source] = 0
		} else if matches := reAlert.FindStringSubmatch(line); len(matches) == 2 {
			source = matches[1]
		} else if matches := reCritical.FindStringSubmatch(line); len(matches) == 2 {
			sinks = strings.Split(matches[1], ", ")
		}
	}
	return g, source, sinks, nil
}
