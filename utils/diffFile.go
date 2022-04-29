package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

type BaF struct {
	before string
	after  string
}

func iterate() {
	file, err := os.Open(`diffile.txt`)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	result, err := os.Create(`diffOptimise.txt`)
	if err != nil {
		log.Fatal(err)
	}
	defer result.Close()

	resultMapa := make(map[string][]BaF)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		lineSplited := strings.Split(line, " >>> ")
		fileName := lineSplited[0]
		lineBefore := lineSplited[1]
		lineAfter := lineSplited[2]
		_, ok := resultMapa[fileName]
		if !ok {
			tmp := BaF{before: lineBefore, after: lineAfter}

			resultMapa[fileName] = append(resultMapa[fileName], tmp)
		} else {
			tmp := BaF{before: lineBefore, after: lineAfter}

			resultMapa[fileName] = append(resultMapa[fileName], tmp)
		}
	}

	log.Println(`scanned file, now make result`)

	for key, val := range resultMapa {
		result.WriteString(fmt.Sprintf("FILENAME = %s\n----------------------------\n", key))
		for _, line := range val {
			result.WriteString(fmt.Sprintf("- %s\n", line.before))
			result.WriteString(fmt.Sprintf("+ %s\n", line.after))
		}
		result.WriteString(fmt.Sprintf("-------------END FILE -------------------\n"))
	}
}

func main() {
	iterate()
}
