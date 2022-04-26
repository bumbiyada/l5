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
		end          = 17211 //93589
		atom   int64 = 0
		errCnt int64 = 0
		//CheckSum         int   = 0
		HowMuchEqualNM   int   = 0
		HowMuchEqualSubj int   = 0
		BothNMandSUBJEQ  int   = 0
		NmEqSubjNot      int   = 0
		TotalNM          int64 = 0
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
		var NmCntList []int64
		var SubjCntList []int64

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
			NmCntList = append(NmCntList, nmCnt)
			SubjCnt, _ := strconv.ParseInt(strings.ReplaceAll(lineSplitted[2], " ", ""), 10, 64)
			SubjCntList = append(SubjCntList, SubjCnt)
			TotalNM += nmCnt
		}
		atom++
		eqNm := CheckEqual(NmCntList...)
		eqSub := CheckEqual(SubjCntList...)
		if eqNm == true {
			HowMuchEqualNM++
		}
		if eqSub == true {
			HowMuchEqualSubj++
		}
		if eqNm == true && eqSub == true {
			BothNMandSUBJEQ++
		}
		if eqNm == true && eqSub == false {
			NmEqSubjNot++
		}
	}
	// result

	fmt.Println("Values ", atom)
	fmt.Printf("\tNM equal | Subj equal\t | Both NM and Subj equal \n\t%v\t | %v\t\t | %v\n", HowMuchEqualNM, HowMuchEqualSubj, BothNMandSUBJEQ)
	fmt.Printf("Differenties\n \t %v \t | %v\t\t | %v\n", int(atom)-HowMuchEqualNM, int(atom)-HowMuchEqualSubj, int(atom)-BothNMandSUBJEQ)
	fmt.Println(NmEqSubjNot)
	fmt.Println(TotalNM)
}

func CheckEqual(intList ...int64) bool {
	var (
		max int64 = intList[0]
		min int64 = intList[0]
	)
	for _, val := range intList {
		if val > max {
			max = val
		}
		if val < min {
			min = val
		}
	}
	if max == min {
		return true
	} else {
		return false
	}
}
func main() {
	getStats()
}
