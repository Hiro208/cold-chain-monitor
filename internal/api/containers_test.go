package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/guangjiandong/cold-chain-monitor/internal/domain"
)

const validContainer = `{"id":"csqu3054383","owner":"acme","commodity":"vaccines","setpoint_c":5,"min_temp_c":2,"max_temp_c":8}`

func TestCreateAndGetContainer(t *testing.T) {
	h := newTestServer(t, newFakeStore())

	rec := do(t, h, http.MethodPost, "/v1/containers", validContainer)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status %d body %s", rec.Code, rec.Body)
	}
	if loc := rec.Header().Get("Location"); loc != "/v1/containers/CSQU3054383" {
		t.Fatalf("Location = %q", loc)
	}
	if rec.Header().Get(requestIDHeader) == "" {
		t.Fatal("missing request id header")
	}

	rec = do(t, h, http.MethodPost, "/v1/containers", validContainer)
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate create: status %d", rec.Code)
	}

	rec = do(t, h, http.MethodGet, "/v1/containers/csqu3054383", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get: status %d", rec.Code)
	}
	var got domain.Container
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != "CSQU3054383" || got.MaxTempC != 8 {
		t.Fatalf("unexpected container %+v", got)
	}

	if rec := do(t, h, http.MethodGet, "/v1/containers/MSKU0000000", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("missing container: status %d", rec.Code)
	}
}

func TestCreateContainerValidation(t *testing.T) {
	h := newTestServer(t, newFakeStore())
	cases := map[string]string{
		"bad json":        `{`,
		"unknown field":   `{"id":"CSQU3054383","owner":"a","setpoint_c":5,"min_temp_c":2,"max_temp_c":8,"x":1}`,
		"missing max":     `{"id":"CSQU3054383","owner":"a","setpoint_c":5,"min_temp_c":2}`,
		"bad check digit": `{"id":"CSQU3054384","owner":"a","setpoint_c":5,"min_temp_c":2,"max_temp_c":8}`,
		"two objects":     validContainer + validContainer,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if rec := do(t, h, http.MethodPost, "/v1/containers", body); rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d body %s", rec.Code, rec.Body)
			}
		})
	}
}

func TestListContainersPagination(t *testing.T) {
	st := newFakeStore()
	h := newTestServer(t, st)
	for i := 0; i < 5; i++ {
		body := fmt.Sprintf(`{"id":%q,"owner":"acme","setpoint_c":-18,"min_temp_c":-25,"max_temp_c":-15}`, domain.MakeContainerID("ABC", i))
		if rec := do(t, h, http.MethodPost, "/v1/containers", body); rec.Code != http.StatusCreated {
			t.Fatalf("seed %d: %d %s", i, rec.Code, rec.Body)
		}
	}

	var seen []string
	cursor := ""
	for pages := 0; pages < 10; pages++ {
		rec := do(t, h, http.MethodGet, "/v1/containers?limit=2&owner=acme&cursor="+cursor, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		var page containerPage
		if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		for _, c := range page.Items {
			seen = append(seen, c.ID)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != 5 {
		t.Fatalf("paged through %d containers, want 5: %v", len(seen), seen)
	}

	if rec := do(t, h, http.MethodGet, "/v1/containers?limit=0", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("limit=0: status %d", rec.Code)
	}
}
