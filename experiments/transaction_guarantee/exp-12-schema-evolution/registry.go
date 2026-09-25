package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	schemas "github.com/DmytroLysenko1/Kafka-lab/proto"
)

var errRegistry = errors.New("exp-12: registry")

const registryTimeout = 10 * time.Second

// baseline is the schema the service actually publishes under, not a paraphrase of it:
// a matrix measured against a simplified copy would answer a question nobody asked.
var baseline = schemas.PaymentEventsV1

const amountField = "  int64 amount_minor = 3;"

// The three changes anyone proposes in a design review, derived from the baseline by edit
// so that they cannot drift away from the schema they are changes to.
var changes = []struct {
	name   string
	schema string
}{
	{
		name: "add an optional field",
		schema: strings.Replace(baseline,
			"  google.protobuf.Timestamp occurred_at = 5;",
			"  google.protobuf.Timestamp occurred_at = 5;\n  string customer_reference = 6;", 1),
	},
	{
		name:   "remove a field",
		schema: strings.Replace(baseline, amountField+"\n", "", 1),
	},
	{
		name:   "retype a field, keeping its number",
		schema: strings.Replace(baseline, amountField, "  string amount_minor = 3;", 1),
	},
}

// Every change has to be a change: a replacement that matched nothing would send the
// baseline back to the registry and report a compatible verdict about nothing.
func init() {
	for _, change := range changes {
		if change.schema == baseline {
			panic("exp-12: the edit for " + change.name + " matched nothing in the baseline schema")
		}
	}
}

var levels = []string{"NONE", "BACKWARD", "FULL"}

// compatibility asks the registry the question everyone assumes the answer to: which of
// these changes will it let through, and at which setting. Every cell gets a subject of its
// own, seeded with the baseline and then set to the level under test — a subject that
// already holds a rejected or accepted version would compare the next change against that
// instead of against the baseline, which is how a first attempt at this produced a matrix
// that looked authoritative and was not.
func compatibility(ctx context.Context, registryURL string, out io.Writer) error {
	written := &lines{out: out}

	// Every cell below sets the level it tests, so none of them can answer the question the
	// write-up leads with: what a registry nobody configured protects. That has to be read
	// off the server before anything is set, or it is an assertion rather than a result.
	if err := reportDefaultLevel(ctx, registryURL, written); err != nil {
		return err
	}

	written.printf("%-38s %-10s %s\n", "change", "level", "registry")

	for _, level := range levels {
		for _, change := range changes {
			verdict, err := ask(ctx, registryURL, level, change.schema)
			if err != nil {
				return err
			}
			written.printf("%-38s %-10s %s\n", change.name, level, verdict)
		}
		written.printf("\n")
	}
	return written.err
}

// reportDefaultLevel registers one subject and asks, before setting anything, what level it
// and the registry as a whole report. A subject nobody configured answering "not configured"
// is itself the finding: until someone sets a level, the checker has nothing to enforce.
func reportDefaultLevel(ctx context.Context, registryURL string, written *lines) error {
	subject := fmt.Sprintf("exp12-default-%d-value", time.Now().UnixNano())
	if err := register(ctx, registryURL, subject, baseline); err != nil {
		return err
	}

	forSubject, err := levelAt(ctx, registryURL, "/config/"+subject)
	if err != nil {
		return err
	}
	registryWide, err := levelAt(ctx, registryURL, "/config")
	if err != nil {
		return err
	}
	if err := deleteSubject(ctx, registryURL, subject); err != nil {
		return err
	}

	written.printf("before any level is set, on a subject just created:\n")
	written.printf("  the subject reports       %s\n", forSubject)
	written.printf("  the registry-wide default %s\n\n", registryWide)
	return written.err
}

// levelAt reports the compatibility level configured at one config path. A 404 is an answer
// rather than a failure: it means nothing is configured there.
func levelAt(ctx context.Context, registryURL, path string) (string, error) {
	status, answer, err := call(ctx, http.MethodGet, registryURL+path, nil)
	if err != nil {
		return "", err
	}
	if status == http.StatusNotFound {
		return "not configured", nil
	}
	var configured struct {
		CompatibilityLevel string `json:"compatibilityLevel"`
	}
	if err := json.Unmarshal(answer, &configured); err != nil {
		return "", fmt.Errorf("%w: reading the level at %s: %w", errRegistry, path, err)
	}
	return configured.CompatibilityLevel, nil
}

// ask puts one change to the registry on a subject of its own, seeded with the baseline
// and set to the level under test.
func ask(ctx context.Context, registryURL, level, schema string) (string, error) {
	subject := fmt.Sprintf("exp12-%d-value", time.Now().UnixNano())

	if err := register(ctx, registryURL, subject, baseline); err != nil {
		return "", err
	}
	if err := setLevel(ctx, registryURL, subject, level); err != nil {
		return "", err
	}
	verdict, err := attempt(ctx, registryURL, subject, schema)
	if err != nil {
		return "", err
	}
	if err := deleteSubject(ctx, registryURL, subject); err != nil {
		return "", err
	}
	return verdict, nil
}

func register(ctx context.Context, registryURL, subject, schema string) error {
	verdict, err := attempt(ctx, registryURL, subject, schema)
	if err != nil {
		return err
	}
	if verdict != "accepted" {
		return fmt.Errorf("%w: the baseline itself was refused: %s", errRegistry, verdict)
	}
	return nil
}

func attempt(ctx context.Context, registryURL, subject, schema string) (string, error) {
	body, err := json.Marshal(map[string]string{"schemaType": "PROTOBUF", "schema": schema})
	if err != nil {
		return "", err
	}

	status, _, err := call(ctx, http.MethodPost, registryURL+"/subjects/"+subject+"/versions", body)
	if err != nil {
		return "", err
	}
	switch status {
	case http.StatusOK:
		return "accepted", nil
	case http.StatusConflict:
		return "refused (409)", nil
	default:
		return fmt.Sprintf("unexpected (%d)", status), nil
	}
}

func setLevel(ctx context.Context, registryURL, subject, level string) error {
	body, err := json.Marshal(map[string]string{"compatibility": level})
	if err != nil {
		return err
	}
	if _, _, err := call(ctx, http.MethodPut, registryURL+"/config/"+subject, body); err != nil {
		return err
	}

	// Read it back rather than trust the write: a level that did not take would turn this
	// whole matrix into a column of one setting measured three times.
	configured, err := levelAt(ctx, registryURL, "/config/"+subject)
	if err != nil {
		return err
	}
	if configured != level {
		return fmt.Errorf("%w: asked for %s, the subject reports %s", errRegistry, level, configured)
	}
	return nil
}

func deleteSubject(ctx context.Context, registryURL, subject string) error {
	_, _, err := call(ctx, http.MethodDelete, registryURL+"/subjects/"+subject, nil)
	return err
}

// call talks to the registry the operator configured. The url comes from the environment,
// the same value the service itself is given, and nothing in this path is caller input.
func call(ctx context.Context, method, url string, body []byte) (status int, answer []byte, err error) {
	timed, cancel := context.WithTimeout(ctx, registryTimeout)
	defer cancel()

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(timed, method, url, reader) //nolint:gosec // operator configuration, not caller input
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	response, err := http.DefaultClient.Do(request) //nolint:gosec // operator configuration, not caller input
	if err != nil {
		return 0, nil, fmt.Errorf("%w: %s: %w", errRegistry, method, err)
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	answer, err = io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return 0, nil, err
	}
	return response.StatusCode, answer, nil
}
