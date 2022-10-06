package bond

import (
	"bytes"
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
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(
			"POST",
			"/bond/indexes",
			bytes.NewBufferString("{\"tableName\": \"token_balance\"}"))

		mux.ServeHTTP(w, req)
		require.Equal(t, 200, w.Code)

		assert.Equal(t,
			"[\"primary\",\"account_address_idx\",\"account_and_contract_address_idx\"]",
			w.Body.String(),
		)
	})

	t.Run("EntryFields", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(
			"POST",
			"/bond/entryFields",
			bytes.NewBufferString("{\"tableName\": \"token_balance\"}"))

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

}
