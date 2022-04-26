package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
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
	NmCnt     int
	SubjCnt   int
	presetId  string
	query     string
	bucket    string
	presetId2 string
}

//
func makeRequest(ourUrl string) (resp *http.Response, err error) {
	for i := 0; i < 5; i++ {
		resp, err := http.Get(ourUrl)
		if err != nil {
			time.Sleep(time.Second)
		} else {
			return resp, nil
		}
	}

	return nil, errors.New("{elasticSearch}: failed to connect to elastic search or wrong query")
}

//
func UrlFormer(input string) (string, error) {
	var (
		urlPattern string = `http://elastic-searcher.wbx-search.svc.k8s.dataline/search?`
		urlArgs    string
	)

	// 2)
	// parse parameters for elastic search with Regexp

	// our regexp
	reg, err := regexp.Compile(`--[^"=]+=([^ "]+|"[^"]+")`)
	if err != nil {
		return "", err
	}

	splitedParams := reg.FindAllString(input, -1)
	for _, param := range splitedParams {
		// split key and val
		keyval := strings.Split(param, `=`)

		// prepare parameter name
		paramName := keyval[0]
		paramName = strings.Replace(paramName, "--", "&", 1)

		// prepare parameter value
		paramVal := keyval[1]
		paramVal = strings.ReplaceAll(paramVal, `"`, ``)
		paramVal = url.QueryEscape(paramVal)

		// append parsed parameter to result string
		urlArgs = urlArgs + paramName + "=" + paramVal
	}
	// final preparations
	urlArgs = strings.TrimPrefix(urlArgs, `&`)

	// 3)
	// return valid string
	return urlPattern + urlArgs + "&fields=subject", nil
}

// elastic
func elasticQuery(query string) (res Data2Compare, err error) {

	var (
		subjMap     = make(map[string]struct{})
		subjCnt int = 0
		NmCnt   int = 0
	)

	// for url
	our_url, err := UrlFormer(query)
	if err != nil {
		return res, err
	}
	// make request, get responce
	resp, err := makeRequest(our_url)
	if err != nil {
		return res, err
	}
	defer resp.Body.Close()

	reader := bufio.NewScanner(resp.Body)
	for reader.Scan() {
		line := reader.Text()
		lineSplited := strings.Split(line, ",")
		// if not valid line from elasticsearch
		if len(lineSplited) != 2 {
			log.Println("Api.{GetSubjs}: wrong count of args in elasticsearch responce")
			return res, err
		}
		NmCnt++
		sub := lineSplited[1]
		// if it`s first time

		subjMap[sub] = struct{}{}
	}

	// make data
	subjCnt = len(subjMap)
	res.NmCnt = NmCnt
	res.SubjCnt = subjCnt
	return res, nil
}

// main function
func iterateFiles(number, start, end int) {

	// variables
	var (
		atom      int64 = 0
		errCnt    int64 = 0
		bucketCnt int   = 0
		CheckSum  int   = 0
		buckList  []buck
	)
	miners := make(map[string]int)
	buckets := make(map[string]int)
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
	go func(atom, errCnt *int64) {
		timer := 0
		for {
			time.Sleep(60 * time.Second)
			timer++
			log.Printf("%v) Scanned = %v, errors = %v\n minutes = %v", number, *atom, *errCnt, timer)
		}

	}(&atom, &errCnt)
	// start iterating over all files
	for i := start; i <= end; i++ {
		var etalon EtalonReq
		var queryList []string
		fileName := strconv.FormatInt(int64(i), 10) + ".csv"

		// open file
		filePath := filepath.Join("", "data", "several", fileName)
		//log.Println(filePath)
		file, err := os.Open(filePath)
		if err != nil {
			logFile.WriteString(fmt.Sprintf("Can`t open file %s\n", fileName))
			errCnt++
			continue
		}
		defer file.Close()

		// MINIRES
		MinifileName := strconv.FormatInt(int64(i), 10) + "_res" + ".csv"
		MinifilePath := filepath.Join("", "data", "results", MinifileName)
		MiniRes, err := os.Create(MinifilePath)
		if err != nil {
			logFile.WriteString(fmt.Sprintf("Can`t create file %s_res\n", fileName))
			errCnt++
			continue
		}
		defer MiniRes.Close()

		// scan file
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			// !
			// each search.csv line

			line := scanner.Text()
			queryList = append(queryList, line)
			lineSplitted := strings.Split(line, "|")
			if len(lineSplitted) != 10 {
				logFile.WriteString(fmt.Sprintf("In file %s error in encoding string --> %s\n", fileName, line))
				continue
			}
			miner := lineSplitted[5]
			miner_args := lineSplitted[6]
			_, ok := miners[miner]
			if !ok {
				miners[miner] = 1
			} else {
				miners[miner]++
			}
			bucket := strings.TrimPrefix(lineSplitted[7], `presets/`)
			_, ok = buckets[bucket]
			if !ok {
				buckets[bucket] = 1
			} else {
				buckets[bucket]++
			}

			if miner == `elasticsearch` {
				data, err := elasticQuery(miner_args)
				if err != nil {
					logFile.WriteString(fmt.Sprintf("In file %s string %s [ERROR] %s\n", fileName, line, err))
					continue
				}
				// minires

				MiniRes.WriteString(fmt.Sprintf("%s >>> %v>>>%v\n", line, data.NmCnt, data.SubjCnt))
				// if data >
				if data.NmCnt >= etalon.NmCnt && data.SubjCnt >= etalon.SubjCnt {
					etalon.NmCnt = data.NmCnt
					etalon.SubjCnt = data.SubjCnt
					etalon.bucket = bucket
					etalon.presetId = lineSplitted[1]
					etalon.presetId2 = lineSplitted[8]
					etalon.query = miner_args
				}
			} else if miner == `elastic` {
				logFile.WriteString(fmt.Sprintf("Elastic Miner In file %s string %s\n", fileName, line))
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
			ResFile.WriteString(fmt.Sprintf("%s >>> %s\n", line, CommitLine))
		}
		log.Printf("File %s done\n", fileName)
	}
	// result
	fmt.Print("Buckets\n\n")
	fileBuckets, err := os.Create("bucketsList.txt")
	if err != nil {
		log.Println("err", err)
	}
	defer fileBuckets.Close()

	for key, val := range buckets {
		tmpBuck := buck{}
		tmpBuck.name = key
		tmpBuck.count = val
		buckList = append(buckList, tmpBuck)
		bucketCnt++
		CheckSum += val
		log.Printf("%s : %v\n", key, val)
		//fileBuckets.WriteString(fmt.Sprintf("%s : %v\n", key, val))
	}
	sort.SliceStable(buckList, func(i, j int) bool { return buckList[i].count < buckList[j].count })
	for _, val := range buckList {
		fileBuckets.WriteString(fmt.Sprintf("%s,", val.name))
	}
	//fmt.Println(bucketCnt)
	fmt.Println("Values ", CheckSum)
	fmt.Print("Miners\n\n")
	for key, val := range miners {
		log.Printf("%s : %v\n", key, val)
	}
	log.Printf("Scanned = %v, errors = %v\n", atom, errCnt)
}
func main() {
	// start main function
	//start           = //10272
	//end             = //93589
	var wg sync.WaitGroup
	wg.Add(6)
	go func() {
		defer wg.Done()
		iterateFiles(1, 76001, 76300)
	}()
	go func() {
		defer wg.Done()
		iterateFiles(2, 76301, 76600) //76000
	}()
	go func() {
		defer wg.Done()
		iterateFiles(3, 76601, 76900)
	}()
	go func() {
		defer wg.Done()
		iterateFiles(4, 76901, 77200)
	}()
	go func() {
		defer wg.Done()
		iterateFiles(5, 77201, 77500)
	}()
	// go func() {
	// 	defer wg.Done()
	// 	iterateFiles(6, 85402, 85500)
	// }()
	// go func() {
	// 	defer wg.Done()
	// 	iterateFiles(7, 85501, 87500)
	// }()
	// go func() {
	// 	defer wg.Done()
	// 	iterateFiles(8, 89470, 89500)
	// }()
	// go func() {
	// 	defer wg.Done()
	// 	iterateFiles(9, 89501, 91500)
	// }()
	go func() {
		defer wg.Done()
		iterateFiles(10, 93314, 93589)
	}()
	wg.Wait()
}
