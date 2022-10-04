package bond

import "net/http"

func NewInspectHandler(inspect Inspect) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		// todo: implement
	}
}
