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
	"strconv"
	"strings"
	"time"
)

type Data2Compare struct {
	NmCnt   int
	SubjCnt int
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
func getFile(src string) {

	// start loging
	logFile, err := os.Create("logfile.txt")
	if err != nil {
		log.Fatalln("Can`t create logfile.txt")
	}
	defer logFile.Close()

	// runtime logger
	log.Println(src)
	fileName := strings.ReplaceAll(src, "_res", "")
	log.Println(fileName)
	n := strings.ReplaceAll(src, "_res.csv", "")
	log.Println(n)
	i, _ := strconv.ParseInt(n, 10, 64)
	log.Println(i)
	// start iterating over all files
	filePath := filepath.Join("", "data", "several", fileName)
	log.Println(filePath)
	//log.Println(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		logFile.WriteString(fmt.Sprintf("Can`t open file %s\n", fileName))

	}
	defer file.Close()

	// MINIRES
	MinifileName := strconv.FormatInt(int64(i), 10) + "_res" + ".csv"
	log.Println(MinifileName)
	MinifilePath := filepath.Join("", "data", "results", MinifileName)
	log.Println(MinifilePath)
	MiniRes, err := os.Create(MinifilePath)
	if err != nil {
		logFile.WriteString(fmt.Sprintf("Can`t create file %s_res\n", fileName))

	}
	defer MiniRes.Close()

	// scan file
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// !
		// each search.csv line

		line := scanner.Text()
		log.Println(line)
		lineSplitted := strings.Split(line, "|")
		if len(lineSplitted) != 10 {
			logFile.WriteString(fmt.Sprintf("In file %s error in encoding string --> %s\n", fileName, line))
			continue
		}
		miner := lineSplitted[5]
		miner_args := lineSplitted[6]

		if miner == `elasticsearch` {
			data, err := elasticQuery(miner_args)
			if err != nil {
				logFile.WriteString(fmt.Sprintf("In file %s string %s [ERROR] %s\n", fileName, line, err))
				continue
			}
			// minires

			MiniRes.WriteString(fmt.Sprintf("%s >>> %v>>>%v\n", line, data.NmCnt, data.SubjCnt))
			log.Println("to minires = ", line, data.NmCnt, data.SubjCnt)
			// if data >

		} else if miner == `elastic` {
			logFile.WriteString(fmt.Sprintf("Elastic Miner In file %s string %s\n", fileName, line))
		}
	}

}

// result

func main() {
	// variables
	var (
		atom   int64 = 0
		errCnt int64 = 0
	)
	file, err := os.Open(`resFile.txt`)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()
	go func(atom, errCnt *int64) {
		timer := 0
		for {
			time.Sleep(60 * time.Second)
			timer++
			log.Printf("Scanned = %v, errors = %v\n minutes = %v", *atom, *errCnt, timer)
		}

	}(&atom, &errCnt)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		getFile(line)
		atom++
	}
}
