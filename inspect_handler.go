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
	Table string `json:"table"`
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
			data, err = json.Marshal(inspect.Indexes(req.Table))
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
			data, err = json.Marshal(inspect.EntryFields(req.Table))
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

type requestQuery struct {
	Table         string                 `json:"table"`
	Index         string                 `json:"index"`
	IndexSelector map[string]interface{} `json:"indexSelector"`
	Filter        map[string]interface{} `json:"filter"`
	Limit         uint64                 `json:"limit"`
	After         map[string]interface{} `json:"after"`
}

func buildQueryHandler(inspect Inspect) http.HandlerFunc {
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

		var req requestQuery
		err = json.Unmarshal(data, &req)
		if err != nil {
			response.WriteHeader(http.StatusInternalServerError)
			return
		}

		switch accept {
		case "application/json":
			result, err := inspect.Query(request.Context(), req.Table, req.Index, req.IndexSelector,
				req.Filter, req.Limit, req.After)
			if err != nil {
				response.WriteHeader(http.StatusInternalServerError)
				return
			}

			data, err = json.Marshal(result)
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
