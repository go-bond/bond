package inspect

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"regexp"
)

const (
	TablesPath      = "/tables"
	IndexesPath     = "/indexes"
	EntryFieldsPath = "/entryFields"
	QueryPath       = "/query"
)

func NewInspectHandler(inspect Inspect) http.HandlerFunc {
	var (
		// path pattern matchers
		endsInTables      = regexp.MustCompile(TablesPath + "$")
		endsInIndexes     = regexp.MustCompile(IndexesPath + "$")
		endsInEntryFields = regexp.MustCompile(EntryFieldsPath + "$")
		endsInQuery       = regexp.MustCompile(QueryPath + "$")

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

type responseError struct {
	Error string `json:"error"`
}

func newResponseError(err error) responseError {
	return responseError{Error: err.Error()}
}

func newResponseErrorBytes(err error) ([]byte, error) {
	respErr := newResponseError(err)
	return json.Marshal(respErr)
}

func buildTablesHandler(inspect Inspect) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		accept := request.Header.Get("Accept")
		if accept == "" {
			accept = "application/json"
		}

		switch accept {
		case "application/json":
			tables, err := inspect.Tables()
			if err != nil {
				writeErrorResponse(response, http.StatusInternalServerError, err)
				return
			}

			data, err := json.Marshal(tables)
			if err != nil {
				writeErrorResponse(response, http.StatusInternalServerError, err)
				return
			}

			writeResponse(response, http.StatusOK, data)
		default:
			writeEmptyResponse(response, http.StatusNotAcceptable)
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
			writeErrorResponse(response, http.StatusInternalServerError, err)
			return
		}

		var req requestIndexes
		err = json.Unmarshal(data, &req)
		if err != nil {
			writeErrorResponse(response, http.StatusInternalServerError, err)
			return
		}

		switch accept {
		case "application/json":
			indexes, err := inspect.Indexes(req.Table)
			if err != nil {
				writeErrorResponse(response, http.StatusInternalServerError, err)
				return
			}

			data, err = json.Marshal(indexes)
			if err != nil {
				writeErrorResponse(response, http.StatusInternalServerError, err)
				return
			}

			writeResponse(response, http.StatusOK, data)
		default:
			writeEmptyResponse(response, http.StatusNotAcceptable)
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
			writeErrorResponse(response, http.StatusInternalServerError, err)
			return
		}

		var req requestEntryFields
		err = json.Unmarshal(data, &req)
		if err != nil {
			writeErrorResponse(response, http.StatusInternalServerError, err)
			return
		}

		switch accept {
		case "application/json":
			fields, err := inspect.EntryFields(req.Table)
			if err != nil {
				writeErrorResponse(response, http.StatusInternalServerError, err)
				return
			}

			data, err = json.Marshal(fields)
			if err != nil {
				writeErrorResponse(response, http.StatusInternalServerError, err)
				return
			}

			writeResponse(response, http.StatusOK, data)
		default:
			writeEmptyResponse(response, http.StatusNotAcceptable)
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
			writeErrorResponse(response, http.StatusInternalServerError, err)
			return
		}

		var req requestQuery
		err = json.Unmarshal(data, &req)
		if err != nil {
			writeErrorResponse(response, http.StatusInternalServerError, err)
			return
		}

		switch accept {
		case "application/json":
			result, err := inspect.Query(request.Context(), req.Table, req.Index, req.IndexSelector,
				req.Filter, req.Limit, req.After)
			if err != nil {
				writeErrorResponse(response, http.StatusInternalServerError, err)
				return
			}

			data, err = json.Marshal(result)
			if err != nil {
				writeErrorResponse(response, http.StatusInternalServerError, err)
				return
			}

			writeResponse(response, http.StatusOK, data)
		default:
			writeEmptyResponse(response, http.StatusNotAcceptable)
		}
	}
}

func writeResponse(response http.ResponseWriter, status int, data []byte) {
	response.WriteHeader(status)
	_, _ = response.Write(data)
}

func writeEmptyResponse(response http.ResponseWriter, status int) {
	response.WriteHeader(status)
}

func writeErrorResponse(response http.ResponseWriter, status int, err error) {
	response.WriteHeader(status)

	errBytes, errErrResp := newResponseErrorBytes(err)
	if errErrResp == nil {
		_, _ = response.Write(errBytes)
	}
}
