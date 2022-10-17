package bond

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewInspectHandler(t *testing.T) {
	db, table, _, _ := setupDatabaseForQuery()
	defer tearDownDatabase(db)

	insp, err := NewInspect([]TableInfo{table})
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.Handle("/bond/", NewInspectHandler(insp))

	t.Run("Tables", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(
			"POST",
			"/bond/tables",
			bytes.NewBufferString(""))

		mux.ServeHTTP(w, req)
		require.Equal(t, 200, w.Code)

		assert.Equal(t, "[\"token_balance\"]", w.Body.String())
	})

	t.Run("Indexes", func(t *testing.T) {
		t.Run("Simple", func(t *testing.T) {
			w := httptest.NewRecorder()
			req, _ := http.NewRequest(
				"POST",
				"/bond/indexes",
				bytes.NewBufferString("{\"table\": \"token_balance\"}"))

			mux.ServeHTTP(w, req)
			require.Equal(t, 200, w.Code)

			assert.Equal(t,
				"[\"primary\",\"account_address_idx\",\"account_and_contract_address_idx\"]",
				w.Body.String(),
			)
		})

		t.Run("ErrorTableNotFound", func(t *testing.T) {
			expectedError := map[string]interface{}{
				"error": "table not found",
			}

			w := httptest.NewRecorder()
			req, _ := http.NewRequest(
				"POST",
				"/bond/indexes",
				bytes.NewBufferString("{\"table\": \"no_such_table\"}"))

			mux.ServeHTTP(w, req)
			require.Equal(t, 500, w.Code)

			var results map[string]interface{}
			data := w.Body.Bytes()
			err = json.Unmarshal(data, &results)
			require.NoError(t, err)

			assert.Equal(t, results, expectedError)
		})
	})

	t.Run("EntryFields", func(t *testing.T) {
		t.Run("Simple", func(t *testing.T) {
			w := httptest.NewRecorder()
			req, _ := http.NewRequest(
				"POST",
				"/bond/entryFields",
				bytes.NewBufferString("{\"table\": \"token_balance\"}"))

			mux.ServeHTTP(w, req)
			require.Equal(t, 200, w.Code)

			expectedEntryFields := map[string]string{
				"AccountAddress":  "string",
				"AccountID":       "uint32",
				"Balance":         "uint64",
				"ContractAddress": "string",
				"ID":              "uint64",
				"TokenID":         "uint32",
			}

			var entryFields map[string]string
			err = json.Unmarshal(w.Body.Bytes(), &entryFields)
			require.NoError(t, err)

			assert.Equal(t, expectedEntryFields, entryFields)
		})

		t.Run("ErrorTableNotFound", func(t *testing.T) {
			expectedError := map[string]interface{}{
				"error": "table not found",
			}

			w := httptest.NewRecorder()
			req, _ := http.NewRequest(
				"POST",
				"/bond/entryFields",
				bytes.NewBufferString("{\"table\": \"no_such_table\"}"))

			mux.ServeHTTP(w, req)
			require.Equal(t, 500, w.Code)

			var results map[string]interface{}
			data := w.Body.Bytes()
			err = json.Unmarshal(data, &results)
			require.NoError(t, err)

			assert.Equal(t, results, expectedError)
		})
	})

	t.Run("Query", func(t *testing.T) {
		insertTokenBalance := []*TokenBalance{
			{
				ID:              1,
				AccountID:       1,
				ContractAddress: "0xc",
				AccountAddress:  "0xa",
				TokenID:         10,
				Balance:         501,
			},
			{
				ID:              2,
				AccountID:       1,
				ContractAddress: "0xc",
				AccountAddress:  "0xa",
				TokenID:         5,
				Balance:         1,
			},
		}

		err = table.Insert(context.Background(), insertTokenBalance)
		require.NoError(t, err)

		t.Run("Simple", func(t *testing.T) {
			expectedTokenBalance := []map[string]interface{}{
				{
					"ID":              float64(1),
					"AccountID":       float64(1),
					"ContractAddress": "0xc",
					"AccountAddress":  "0xa",
					"TokenID":         float64(10),
					"Balance":         float64(501),
				},
				{
					"ID":              float64(2),
					"AccountID":       float64(1),
					"ContractAddress": "0xc",
					"AccountAddress":  "0xa",
					"TokenID":         float64(5),
					"Balance":         float64(1),
				},
			}

			w := httptest.NewRecorder()
			req, _ := http.NewRequest(
				"POST",
				"/bond/query",
				bytes.NewBufferString("{\"table\": \"token_balance\"}"))

			mux.ServeHTTP(w, req)
			require.Equal(t, 200, w.Code)

			var results []map[string]interface{}
			err = json.Unmarshal(w.Body.Bytes(), &results)
			require.NoError(t, err)

			assert.Equal(t, results, expectedTokenBalance)
		})

		t.Run("SimpleWithLimit", func(t *testing.T) {
			expectedTokenBalance := []map[string]interface{}{
				{
					"ID":              float64(1),
					"AccountID":       float64(1),
					"ContractAddress": "0xc",
					"AccountAddress":  "0xa",
					"TokenID":         float64(10),
					"Balance":         float64(501),
				},
			}

			w := httptest.NewRecorder()
			req, _ := http.NewRequest(
				"POST",
				"/bond/query",
				bytes.NewBufferString("{\"table\": \"token_balance\", \"limit\": 1}"))

			mux.ServeHTTP(w, req)
			require.Equal(t, 200, w.Code)

			var results []map[string]interface{}
			err = json.Unmarshal(w.Body.Bytes(), &results)
			require.NoError(t, err)

			assert.Equal(t, results, expectedTokenBalance)
		})

		t.Run("SimpleWithFilter", func(t *testing.T) {
			expectedTokenBalance := []map[string]interface{}{
				{
					"ID":              float64(1),
					"AccountID":       float64(1),
					"ContractAddress": "0xc",
					"AccountAddress":  "0xa",
					"TokenID":         float64(10),
					"Balance":         float64(501),
				},
			}

			requestBody := map[string]interface{}{
				"table": "token_balance",
				"filter": map[string]interface{}{
					"ID": uint64(1),
				},
			}

			requestBodyData, err := json.Marshal(requestBody)
			require.NoError(t, err)

			w := httptest.NewRecorder()
			req, _ := http.NewRequest(
				"POST",
				"/bond/query",
				bytes.NewBuffer(requestBodyData))

			mux.ServeHTTP(w, req)
			require.Equal(t, 200, w.Code)

			var results []map[string]interface{}
			err = json.Unmarshal(w.Body.Bytes(), &results)
			require.NoError(t, err)

			assert.Equal(t, results, expectedTokenBalance)
		})

		t.Run("SimpleWithSecondaryIndex", func(t *testing.T) {
			expectedTokenBalance := []map[string]interface{}{
				{
					"ID":              float64(1),
					"AccountID":       float64(1),
					"ContractAddress": "0xc",
					"AccountAddress":  "0xa",
					"TokenID":         float64(10),
					"Balance":         float64(501),
				},
				{
					"ID":              float64(2),
					"AccountID":       float64(1),
					"ContractAddress": "0xc",
					"AccountAddress":  "0xa",
					"TokenID":         float64(5),
					"Balance":         float64(1),
				},
			}

			requestBody := map[string]interface{}{
				"table": "token_balance",
				"index": "account_address_idx",
				"indexSelector": map[string]interface{}{
					"AccountAddress": "0xa",
				},
			}

			requestBodyData, err := json.Marshal(requestBody)
			require.NoError(t, err)

			w := httptest.NewRecorder()
			req, _ := http.NewRequest(
				"POST",
				"/bond/query",
				bytes.NewBuffer(requestBodyData))

			mux.ServeHTTP(w, req)
			require.Equal(t, 200, w.Code)

			var results []map[string]interface{}
			err = json.Unmarshal(w.Body.Bytes(), &results)
			require.NoError(t, err)

			assert.Equal(t, results, expectedTokenBalance)

			requestBody = map[string]interface{}{
				"table": "token_balance",
				"index": "account_address_idx",
				"indexSelector": map[string]interface{}{
					"AccountAddress": "0xb",
				},
			}

			requestBodyData, err = json.Marshal(requestBody)
			require.NoError(t, err)

			w = httptest.NewRecorder()
			req, _ = http.NewRequest(
				"POST",
				"/bond/query",
				bytes.NewBuffer(requestBodyData))

			mux.ServeHTTP(w, req)
			require.Equal(t, 200, w.Code)

			err = json.Unmarshal(w.Body.Bytes(), &results)
			require.NoError(t, err)

			assert.Equal(t, results, []map[string]interface{}{})
		})

		t.Run("SimpleWithAfter", func(t *testing.T) {
			expectedTokenBalance := []map[string]interface{}{
				{
					"ID":              float64(1),
					"AccountID":       float64(1),
					"ContractAddress": "0xc",
					"AccountAddress":  "0xa",
					"TokenID":         float64(10),
					"Balance":         float64(501),
				},
			}

			expectedTokenBalance2 := []map[string]interface{}{
				{
					"ID":              float64(2),
					"AccountID":       float64(1),
					"ContractAddress": "0xc",
					"AccountAddress":  "0xa",
					"TokenID":         float64(5),
					"Balance":         float64(1),
				},
			}

			w := httptest.NewRecorder()
			req, _ := http.NewRequest(
				"POST",
				"/bond/query",
				bytes.NewBufferString("{\"table\": \"token_balance\", \"limit\": 1}"))

			mux.ServeHTTP(w, req)
			require.Equal(t, 200, w.Code)

			var results []map[string]interface{}
			err = json.Unmarshal(w.Body.Bytes(), &results)
			require.NoError(t, err)

			assert.Equal(t, results, expectedTokenBalance)

			requestBody := map[string]interface{}{
				"table": "token_balance",
				"after": results[0],
			}

			requestBodyData, err := json.Marshal(requestBody)
			require.NoError(t, err)

			w = httptest.NewRecorder()
			req, _ = http.NewRequest(
				"POST",
				"/bond/query",
				bytes.NewBuffer(requestBodyData))

			mux.ServeHTTP(w, req)
			require.Equal(t, 200, w.Code)

			err = json.Unmarshal(w.Body.Bytes(), &results)
			require.NoError(t, err)

			assert.Equal(t, results, expectedTokenBalance2)
		})

		t.Run("ErrorTableNotFound", func(t *testing.T) {
			expectedError := map[string]interface{}{
				"error": "table not found",
			}

			w := httptest.NewRecorder()
			req, _ := http.NewRequest(
				"POST",
				"/bond/query",
				bytes.NewBufferString("{\"table\": \"no_such_table\", \"limit\": 1}"))

			mux.ServeHTTP(w, req)
			require.Equal(t, 500, w.Code)

			var results map[string]interface{}
			data := w.Body.Bytes()
			err = json.Unmarshal(data, &results)
			require.NoError(t, err)

			assert.Equal(t, results, expectedError)
		})
	})

}
