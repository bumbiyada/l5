package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func getStats() {
	// variables
	var (
		start        = 10272 //10272
		end          = 93589 //93589
		atom   int64 = 0
		errCnt int64 = 0
	)

	// start loging
	logFile, err := os.Create("logfile.txt")
	if err != nil {
		log.Fatalln("Can`t create logfile.txt")
	}
	defer logFile.Close()

	resFile, err := os.Create("resFile.txt")
	if err != nil {
		log.Fatalln("Can`t create resFile.txt")
	}
	defer resFile.Close()
	// runtime logger
	go func(atom, errCnt *int64) {

		for {
			time.Sleep(60 * time.Second)
			log.Printf("Scanned = %v, errors = %v\n", *atom, *errCnt)
			return
		}

	}(&atom, &errCnt)
	// start iterating over all files
	for i := start; i <= end; i++ {

		fileName := strconv.FormatInt(int64(i), 10) + "_res.csv"

		// open file
		filePath := filepath.Join("", "data", "results", fileName)
		//log.Println(filePath)
		file, err := os.Open(filePath)
		if err != nil {
			logFile.WriteString(fmt.Sprintf("Can`t open file %s\n", fileName))
			errCnt++
			continue
		}
		defer file.Close()

		// scan file
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			// !
			// each search.csv line

			line := scanner.Text()

			lineSplitted := strings.Split(line, ">>>")
			if len(lineSplitted) != 3 {
				logFile.WriteString(fmt.Sprintf("In file %s error in encoding string --> %s\n", fileName, line))
				continue
			}
			nmCnt, _ := strconv.ParseInt(strings.ReplaceAll(lineSplitted[1], " ", ""), 10, 64)

			SubjCnt, _ := strconv.ParseInt(strings.ReplaceAll(lineSplitted[2], " ", ""), 10, 64)

			if nmCnt == 0 || SubjCnt == 0 {
				resFile.WriteString(fmt.Sprintf("%s\n", fileName))
				fmt.Printf("In file %s line --> %s\n", fileName, line)
			}
		}
		atom++

	}
	// result

}

func main() {
	getStats()
}
