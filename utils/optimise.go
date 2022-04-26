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

//
// func makeRequest(ourUrl string) (resp *http.Response, err error) {
// 	for i := 0; i < 5; i++ {
// 		resp, err := http.Get(ourUrl)
// 		if err != nil {
// 			time.Sleep(time.Second)
// 		} else {
// 			return resp, nil
// 		}
// 	}

// 	return nil, errors.New("{elasticSearch}: failed to connect to elastic search")
// }

//

// main function
func iterateFiles2() {

	// variables
	var (
		start             = 10272 //10272
		end               = 93589 //93589
		atom     int64    = 0
		errCnt   int64    = 0
		CheckSum int      = 0
		buckList []string = strings.Split(`bucket_196,bucket_215,bucket_45,bucket_12,bucket_214,bucket_213,bucket_47,bucket_101,bucket_197,bucket_195,bucket_48,bucket_203,bucket_10,bucket_42,bucket_26,bucket_189,bucket_43,bucket_39,bucket_210,bucket_30,bucket_58,bucket_16,bucket_185,bucket_85,bucket_78,bucket_27,bucket_17,bucket_188,bucket_208,bucket_137,bucket_150,bucket_152,bucket_29,bucket_151,bucket_100,bucket_129,bucket_187,bucket_204,bucket_1,bucket_28,bucket_164,bucket_84,bucket_183,bucket_102,bucket_146,bucket_22,bucket_180,bucket_178,bucket_14,bucket_133,bucket_38,bucket_115,bucket_105,bucket_32,bucket_3,bucket_79,bucket_136,bucket_36,bucket_19,bucket_2,bucket_156,bucket_6,bucket_68,bucket_110,bucket_21,bucket_70,bucket_148,bucket_23,bucket_37,bucket_139,bucket_18,bucket_40,bucket_111,bucket_76,bucket_25,bucket_117,bucket_8,bucket_171,bucket_170,bucket_15,bucket_107,bucket_35,bucket_72,bucket_132,bucket_135,bucket_86,bucket_127,bucket_167,bucket_192,bucket_77,bucket_145,bucket_184,bucket_182,bucket_112,bucket_106,bucket_90,bucket_41,bucket_181,bucket_24,bucket_176,bucket_109,bucket_166,bucket_73,bucket_20,bucket_13,bucket_175,bucket_123,bucket_87,bucket_119,bucket_75,bucket_71,bucket_141,bucket_95,bucket_69,bucket_131,bucket_88,bucket_93,bucket_108,bucket_174,bucket_134,bucket_74,bucket_5,bucket_83,bucket_169,bucket_94,bucket_147,bucket_168,bucket_118,bucket_89,bucket_172,bucket_144,bucket_103,bucket_177,bucket_143,bucket_161,bucket_155,bucket_154,bucket_82,bucket_163,bucket_97,bucket_113,bucket_114,bucket_116,bucket_142,bucket_162,bucket_159,bucket_126,bucket_128,bucket_160,bucket_173,bucket_7,bucket_158,bucket_91,bucket_104,bucket_130,bucket_125,bucket_11,bucket_153,bucket_121,bucket_96,bucket_165,bucket_92,bucket_157,bucket_149,bucket_120,bucket_138,bucket_207,bucket_124,bucket_122,bucket_99,bucket_179,bucket_98`, ",")
		flag     bool     = false // if true - bucket is big and it will be localy downloaded
	)
	miners := make(map[string]int)
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
		}

	}(&atom, &errCnt)
	// start iterating over all files
	for i := start; i <= end; i++ {

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
			_, ok := miners[miner]
			if !ok {
				miners[miner] = 1
			} else {
				miners[miner]++
			}
			bucket := strings.TrimPrefix(lineSplitted[7], `presets/`)

			// if miner = elastic search = elasticQuery()
			if miner == `elasticsearch` {

				if err != nil {
					logFile.WriteString(fmt.Sprintf("In file %s string %s [ERROR] %s\n", fileName, line, err))
					continue
				}

				for _, val := range buckList {
					if bucket == val {
						flag = true
						break
					}
				}

			} else if miner == `elastic` {
				logFile.WriteString(fmt.Sprintf("Elastic Miner In file %s string %s\n", fileName, line))
			}
		}
		atom++
		if flag == false {
			resFile.WriteString(fmt.Sprintf("%s\n", fileName))
		} else {
			flag = false
		}
	}
	// result

	fmt.Println("Values ", CheckSum)
	fmt.Print("Miners\n\n")
	for key, val := range miners {
		log.Printf("%s : %v\n", key, val)
	}
	log.Printf("Scanned = %v, errors = %v\n", atom, errCnt)
}
func main_r() {
	// start main function
	iterateFiles2()
}
