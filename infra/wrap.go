package infra

import (
	"bytes"
	"net/http"
)

func MakeHttpRequest(method string, url string, beforeFn func(reader *bytes.Buffer), AfterFn func(response *http.Response)) {
	var body bytes.Buffer
	beforeFn(&body)
	req, err := http.NewRequest(method, url, &body)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json;charset=utf-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		panic("http status code is not 200")
	}
	AfterFn(resp)
}
