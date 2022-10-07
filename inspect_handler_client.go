package bond

import "context"

type inspectClient struct {
}

func NewInspectRemote(url string) Inspect {
	return &inspectClient{}
}

func (i inspectClient) Tables() []string {
	//TODO implement me
	panic("implement me")
}

func (i inspectClient) Indexes(table string) []string {
	//TODO implement me
	panic("implement me")
}

func (i inspectClient) EntryFields(table string) map[string]string {
	//TODO implement me
	panic("implement me")
}

func (i inspectClient) Query(ctx context.Context, table string, index string, indexSelector map[string]interface{}, filter map[string]interface{}, limit uint64, after map[string]interface{}) ([]map[string]interface{}, error) {
	//TODO implement me
	panic("implement me")
}
