package bond

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"regexp"
)

func NewInspectHandler(inspect Inspect) http.HandlerFunc {
	var (
		// path pattern matchers
		endsInTables      = regexp.MustCompile("/tables$")
		endsInIndexes     = regexp.MustCompile("/indexes$")
		endsInEntryFields = regexp.MustCompile("/entryFields$")
		endsInQuery       = regexp.MustCompile("/query$")

		// handlers
		tablesHandler      = buildTablesHandler(inspect)
		indexesHandler     = buildIndexesHandler(inspect)
		entryFieldsHandler = buildEntryFieldsHandler(inspect)
		queryHandler       = buildQueryHandler(inspect)
	)

	return func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case endsInTables.Match([]byte(request.URL.Path)):
			tablesHandler.ServeHTTP(writer, request)
		case endsInIndexes.Match([]byte(request.URL.Path)):
			indexesHandler.ServeHTTP(writer, request)
		case endsInEntryFields.Match([]byte(request.URL.Path)):
			entryFieldsHandler.ServeHTTP(writer, request)
		case endsInQuery.Match([]byte(request.URL.Path)):
			queryHandler.ServeHTTP(writer, request)
		default:
			http.NotFound(writer, request)
		}
	}
}

func buildTablesHandler(inspect Inspect) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		accept := request.Header.Get("Accept")
		if accept == "" {
			accept = "application/json"
		}

		switch accept {
		case "application/json":
			data, err := json.Marshal(inspect.Tables())
			if err != nil {
				response.WriteHeader(http.StatusInternalServerError)
				return
			}

			_, _ = response.Write(data)
		default:
			response.WriteHeader(http.StatusNotAcceptable)
		}
	}
}

type requestIndexes struct {
	TableName string `json:"tableName"`
}

func buildIndexesHandler(inspect Inspect) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		accept := request.Header.Get("Accept")
		if accept == "" {
			accept = "application/json"
		}

		data, err := ioutil.ReadAll(request.Body)
		if err != nil {
			response.WriteHeader(http.StatusInternalServerError)
			return
		}

		var req requestIndexes
		err = json.Unmarshal(data, &req)
		if err != nil {
			response.WriteHeader(http.StatusInternalServerError)
			return
		}

		switch accept {
		case "application/json":
			data, err = json.Marshal(inspect.Indexes(req.TableName))
			if err != nil {
				response.WriteHeader(http.StatusInternalServerError)
				return
			}

			_, _ = response.Write(data)
		default:
			response.WriteHeader(http.StatusNotAcceptable)
		}
	}
}

type requestEntryFields struct {
	requestIndexes
}

func buildEntryFieldsHandler(inspect Inspect) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		accept := request.Header.Get("Accept")
		if accept == "" {
			accept = "application/json"
		}

		data, err := ioutil.ReadAll(request.Body)
		if err != nil {
			response.WriteHeader(http.StatusInternalServerError)
			return
		}

		var req requestEntryFields
		err = json.Unmarshal(data, &req)
		if err != nil {
			response.WriteHeader(http.StatusInternalServerError)
			return
		}

		switch accept {
		case "application/json":
			data, err = json.Marshal(inspect.EntryFields(req.TableName))
			if err != nil {
				response.WriteHeader(http.StatusInternalServerError)
				return
			}

			_, _ = response.Write(data)
		default:
			response.WriteHeader(http.StatusNotAcceptable)
		}
	}
}

func buildQueryHandler(inspect Inspect) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
	}
}
