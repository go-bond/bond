package serializers

import "encoding/json"

type JsonSerializer struct {
}

func (s *JsonSerializer) Serialize(i interface{}) ([]byte, error) {
	return json.Marshal(i)
}

func (s *JsonSerializer) Deserialize(b []byte, i interface{}) error {
	return json.Unmarshal(b, i)
}
