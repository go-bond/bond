package bond

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-resty/resty/v2"
)

type inspectClient struct {
	client *resty.Client

	baseURL string
	headers map[string]string

	tablesURL      string
	indexesURL     string
	entryFieldsURL string
	queryURL       string
}

func NewInspectRemote(url string, headers map[string]string) Inspect {
	if strings.HasSuffix(url, "/") {
		url = strings.TrimSuffix(url, "/")
	}

	return &inspectClient{
		client:         resty.New(),
		baseURL:        url,
		headers:        headers,
		tablesURL:      fmt.Sprintf("%s%s", url, TablesPath),
		indexesURL:     fmt.Sprintf("%s%s", url, IndexesPath),
		entryFieldsURL: fmt.Sprintf("%s%s", url, EntryFieldsPath),
		queryURL:       fmt.Sprintf("%s%s", url, QueryPath),
	}
}

func (i *inspectClient) Tables() ([]string, error) {
	resp, err := i.client.R().
		SetHeaders(i.headers).
		Post(i.tablesURL)
	if err != nil {
		return nil, err
	}

	if resp.IsError() {
		return nil, fmt.Errorf("request failed with status: %s", resp.Status())
	}

	var result []string
	err = json.Unmarshal(resp.Body(), &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (i *inspectClient) Indexes(table string) ([]string, error) {
	rqStruct := requestIndexes{Table: table}

	rqData, err := json.Marshal(rqStruct)
	if err != nil {
		return nil, err
	}

	resp, err := i.client.R().
		SetHeaders(i.headers).
		SetBody(rqData).
		Post(i.indexesURL)
	if err != nil {
		return nil, err
	}

	if resp.IsError() {
		return nil, fmt.Errorf("request failed with status: %s", resp.Status())
	}

	var result []string
	err = json.Unmarshal(resp.Body(), &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (i *inspectClient) EntryFields(table string) (map[string]string, error) {
	rqStruct := requestEntryFields{requestIndexes: requestIndexes{Table: table}}

	rqData, err := json.Marshal(rqStruct)
	if err != nil {
		return nil, err
	}

	resp, err := i.client.R().
		SetHeaders(i.headers).
		SetBody(rqData).
		Post(i.entryFieldsURL)
	if err != nil {
		return nil, err
	}

	if resp.IsError() {
		return nil, fmt.Errorf("request failed with status: %s", resp.Status())
	}

	var result map[string]string
	err = json.Unmarshal(resp.Body(), &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (i *inspectClient) Query(ctx context.Context, table string, index string, indexSelector map[string]interface{}, filter map[string]interface{}, limit uint64, after map[string]interface{}) ([]map[string]interface{}, error) {
	rqStruct := requestQuery{
		Table:         table,
		Index:         index,
		IndexSelector: indexSelector,
		Filter:        filter,
		Limit:         limit,
		After:         after,
	}

	rqData, err := json.Marshal(rqStruct)
	if err != nil {
		return nil, err
	}

	resp, err := i.client.R().
		SetHeaders(i.headers).
		SetBody(rqData).
		Post(i.queryURL)
	if err != nil {
		return nil, err
	}

	if resp.IsError() {
		return nil, fmt.Errorf("request failed with status: %s", resp.Status())
	}

	var result []map[string]interface{}
	err = json.Unmarshal(resp.Body(), &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
