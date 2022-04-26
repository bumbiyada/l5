package main

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// global vars
type (
	ToReplaceType struct {
		lineBefore string
		lineAfter  string
	}

	FilesSuccessType struct {
		FileName   string
		SuccessCnt int
	}
)

var (
	ToReplace      = make(chan ToReplaceType)
	ToReplaceResp  = make(chan error)
	ToReplaceClose = make(chan struct{})
)

func MakePullRequest() {
	var (
		errCnt     int
		DoneCnt    int
		SuccessCnt int
	)
	// log file
	logFile, err := os.Create("logfile.txt")
	if err != nil {
		log.Fatalln("Can`t create logfile.txt")
	}
	defer logFile.Close()

	// result file
	SrcFile, err := os.Open("ResFile.txt")
	if err != nil {
		log.Fatalln("Can`t open ResFile.txt")
	}
	defer SrcFile.Close()

	go func(er, done, succ *int) {
		log.Printf("\t[STARTING LOGGER]\n")
		for {
			time.Sleep(30 * time.Second)
			log.Printf("\t[done] = %v,  \t[err] = %v,\t[success] = %v\n", *done, *er, *succ)
		}
	}(&errCnt, &DoneCnt, &SuccessCnt)

	scanner := bufio.NewScanner(SrcFile)
	// ResFile.txt
	// deffault string >>> replacing string
	// iterate ResFile - this is our source file,
	for scanner.Scan() {
		line := scanner.Text()
		lineSplited := strings.Split(line, " >>> ")
		if len(lineSplited) != 2 {
			logFile.WriteString(fmt.Sprintf("[Wrong count of lines] Line:%s\n", line))
			os.Exit(1)
		}
		lineBefore := strings.TrimSuffix(lineSplited[0], " ")
		lineAfter := strings.TrimSuffix(lineSplited[1], " ")

		ReplaceBody := ToReplaceType{lineBefore: lineBefore, lineAfter: lineAfter}
		ToReplace <- ReplaceBody
		err := <-ToReplaceResp
		DoneCnt++
		if err != nil {
			logFile.WriteString(fmt.Sprintf("[DID`t FIND] Line:%s\n", line))
			errCnt++
			//log.Printf("[DID`t FIND] Line:%s\n", line)
		} else {
			SuccessCnt++
		}
		// time.Sleep(time.Second * 1)

	}
	log.Printf("\t[done] = %v,  \t[err] = %v,  \t[success] = %v\n", DoneCnt, errCnt, SuccessCnt)
	ToReplaceClose <- struct{}{}

	//log.Println("\t[EXIT] MAIN FUNCTION")
}

func SearchReplace() {
	// open all files
	startTime := time.Now()
	fileNames := strings.Split(`ru\search-ab.csv, ru\search.csv, common\search\beauty\search.csv, common\search\books\search.csv, common\search\books\search-other.csv, common\search\brands\search.csv, common\search\dresses\bucket-25-dress-search.csv, common\search\dresses\bucket-25-sundress-search.csv, common\search\dresses\bucket-68-dress-search.csv, common\search\dresses\bucket-68-sundress-search.csv, common\search\dresses\dresses-shard-search.csv, common\search\dresses\search-other.csv, common\search\electronics\search.csv, common\search\electronics\search-other.csv, common\search\hats\search.csv, common\search\hats\search-other.csv, common\search\household-goods\search.csv, common\search\household-goods\search-other.csv, common\search\jewelry\search.csv, common\search\jewelry\search-other.csv, common\search\t-shirts\search.csv, common\search\t-shirts\search-other.csv, common\search\underwear\search.csv, common\indices\presets.csv, common\indices\books.csv`, ", ")
	fileArray := [][]string{}
	SuccessArray := []FilesSuccessType{}
	for _, fileName := range fileNames {
		filePath := filepath.Join("data", fileName)
		input, err := ioutil.ReadFile(filePath)
		if err != nil {
			log.Fatal(err)
		}
		SuccesFile := FilesSuccessType{}
		SuccesFile.FileName = fileName
		SuccesFile.SuccessCnt = 0
		SuccessArray = append(SuccessArray, SuccesFile)
		lines := strings.Split(string(input), "\n")
		fileArray = append(fileArray, lines)
		log.Printf("\t[OPEN FILE] : %s, lines = %v\n", filePath, len(lines))

	}

	time.Sleep(time.Second * 3)
	for {
		select {
		case ReplaceBody := <-ToReplace:
			// do something
			// log.Println("[GOT] ", ReplaceBody.lineBefore)
			isFind := false
			for i, file := range fileArray {
				if isFind == true {
					break
				}
				for j, line := range file {

					if line == ReplaceBody.lineBefore {
						fileArray[i][j] = ReplaceBody.lineAfter
						//log.Println("\t[REPLACE] line:", ReplaceBody.lineAfter)
						SuccessArray[i].SuccessCnt++
						ToReplaceResp <- nil
						isFind = true
						break
					}

				}

			}
			// if no file
			if isFind == false {
				ToReplaceResp <- errors.New("no string find")
			}

		case <-ToReplaceClose:
			for i, lines := range fileArray {
				filePath := filepath.Join("results", "data", fileNames[i])
				output := strings.Join(lines, "\n")
				err := ioutil.WriteFile(filePath, []byte(output), 0644)
				if err != nil {
					log.Fatalln(err)
				}
				log.Printf("[SAVE] Filename = %s", filePath)
			}
			dur := time.Since(startTime)
			log.Println("\t[TIME] = ", dur)
			for _, val := range SuccessArray {
				log.Printf("\t[SUCCESSES] = %v, \t[FILE] = %s \n", val.SuccessCnt, val.FileName)
			}
			log.Println("\t[EXIT]")
			return
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
func main() {
	log.Println("[STARTING APP]")
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		SearchReplace()
	}()
	MakePullRequest()
	wg.Wait()
}
