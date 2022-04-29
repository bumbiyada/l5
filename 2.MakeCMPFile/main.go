package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type buck struct {
	name  string
	count int
}

type Data2Compare struct {
	NmCnt   int
	SubjCnt int
}

type EtalonReq struct {
	NmCnt     int64
	SubjCnt   int64
	presetId  string
	query     string
	bucket    string
	presetId2 string
}

//

// elastic

// main function
func iterateFiles(start, end int) {

	// variables
	var (
		atom     int64 = 0
		errCnt   int64 = 0
		CheckSum int   = 0
	)
	// start loging
	logFile, err := os.Create("logfile.txt")
	if err != nil {
		log.Fatalln("Can`t create logfile.txt")
	}
	defer logFile.Close()

	// result file
	ResFile, err := os.Create("ResFile.txt")
	if err != nil {
		log.Fatalln("Can`t create ResFile.txt")
	}
	defer ResFile.Close()
	// runtime logger

	// start iterating over all files
	for i := start; i <= end; i++ {
		var etalon EtalonReq
		var queryList []string
		fileName := strconv.FormatInt(int64(i), 10) + "_res.csv"

		// open file
		filePath := filepath.Join("", "results", fileName)
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

			lineS := strings.Split(line, ">>>")
			if len(lineS) != 3 {
				logFile.WriteString(fmt.Sprintf("In file %s error in encoding string --> %s\n", fileName, line))
				continue
			}
			nmCnt, _ := strconv.ParseInt(strings.ReplaceAll(lineS[1], " ", ""), 10, 64)

			SubjCnt, _ := strconv.ParseInt(strings.ReplaceAll(lineS[2], " ", ""), 10, 64)
			queryList = append(queryList, lineS[0])
			lineSplitted := strings.Split(lineS[0], "|")
			if len(lineSplitted) != 10 {
				logFile.WriteString(fmt.Sprintf("In file %s error in encoding string --> %s\n", fileName, line))
				continue
			}
			// if data >
			if nmCnt >= etalon.NmCnt && SubjCnt >= etalon.SubjCnt {
				etalon.NmCnt = nmCnt
				etalon.SubjCnt = SubjCnt
				etalon.bucket = lineSplitted[7]
				etalon.presetId = lineSplitted[1]
				etalon.presetId2 = lineSplitted[8]
				etalon.query = lineSplitted[6]
			}

		}
		atom++
		for _, line := range queryList {
			lineSplitted := strings.Split(line, "|")
			if len(lineSplitted) != 10 {
				logFile.WriteString(fmt.Sprintf("In file %s error in encoding string --> %s\n", fileName, line))
				continue
			}
			lineSplitted[1] = etalon.presetId
			lineSplitted[6] = etalon.query
			lineSplitted[7] = etalon.bucket
			lineSplitted[8] = etalon.presetId2
			CommitLine := strings.Join(lineSplitted, "|")
			log.Println(line, CommitLine)
			ResFile.WriteString(fmt.Sprintf("%s >>> %s\n", line, CommitLine))
		}
		log.Printf("File %s done\n", fileName)
	}
	// result

	fmt.Println("Values ", CheckSum)

	log.Printf("Scanned = %v, errors = %v\n", atom, errCnt)
}
func main() {
	// start main function
	//start           = //10272
	//end             = //93589
	iterateFiles(10272, 93589)

}
