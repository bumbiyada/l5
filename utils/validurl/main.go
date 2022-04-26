package main

import (
	"log"
	"net/url"
	"regexp"
	"strings"
)

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
	return urlPattern + urlArgs, nil
}
func main() {
	str := `(книга история искусств)|10185648|yes|common||elasticsearch|--query="книга история искусств" --filter="parentSubject:("книжная продукция и диски")"|presets/bucket_53|preset=10185648|()`
	url, _ := UrlFormer(str)
	log.Println(url)
}
