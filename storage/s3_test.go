package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestUnquotedIfMatchSingleAttemptPreservesCAS(t *testing.T) {
	var value, etag string
	var rejectedQuoted int
	var putRequests int
	conflict := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusPreconditionFailed)
		fmt.Fprint(w, "<Error><Code>PreconditionFailed</Code></Error>")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/manifest") {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			if etag == "" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("ETag", etag)
			fmt.Fprint(w, value)
		case http.MethodPut:
			putRequests++
			if r.Header.Get("If-None-Match") == "*" && etag != "" {
				conflict(w)
				return
			}
			if match := r.Header.Get("If-Match"); match != "" {
				if strings.HasPrefix(match, "\"") {
					rejectedQuoted++
					conflict(w)
					return
				}
				if match != strings.Trim(etag, "\"") {
					conflict(w)
					return
				}
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read request: %v", err)
				return
			}
			value = string(body)
			etag = fmt.Sprintf("\"version-%s\"", value)
			w.Header().Set("ETag", etag)
		default:
			http.Error(w, "unsupported", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	cfg := aws.Config{Region: "us-east-1", Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("test", "test", ""))}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
	store := &S3{client: client, bucket: "bucket"}
	ctx := context.Background()
	empty := ""
	if _, err := store.Put(ctx, "manifest", []byte("one"), &empty); err != nil {
		t.Fatal(err)
	}
	_, version, err := store.Get(ctx, "manifest")
	if err != nil || version != `"version-one"` {
		t.Fatalf("GET returned version %q: %v", version, err)
	}
	if _, err := store.Put(ctx, "manifest", []byte("two"), &version); err != nil {
		t.Fatalf("Ceph-style unquoted If-Match failed: %v", err)
	}
	if rejectedQuoted != 0 || putRequests != 2 {
		t.Fatalf("unexpected quoted or repeated PUT: quoted=%d requests=%d", rejectedQuoted, putRequests)
	}
	_, latest, err := store.Get(ctx, "manifest")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "manifest", []byte("three"), &latest); err != nil {
		t.Fatal(err)
	}
	if rejectedQuoted != 0 || putRequests != 3 {
		t.Fatal("subsequent PUT did not use one unquoted If-Match")
	}
	if _, err := store.Put(ctx, "manifest", []byte("stale"), &version); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale If-Match unexpectedly succeeded: %v", err)
	}
	if rejectedQuoted != 0 || putRequests != 4 {
		t.Fatalf("conflict caused a retry: quoted=%d requests=%d", rejectedQuoted, putRequests)
	}
	if got, _, err := store.Get(ctx, "manifest"); err != nil || string(got) != "three" {
		t.Fatalf("stale writer changed manifest: %s, %v", got, err)
	}
}

func TestRangeGetRejectsIgnoredRange(t *testing.T) {
	requests := 0
	serveRange := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if got := r.Header.Get("Range"); got != "bytes=2-4" {
			t.Errorf("range header: %q", got)
		}
		if serveRange {
			w.Header().Set("Content-Range", "bytes 2-4/6")
			w.WriteHeader(http.StatusPartialContent)
			fmt.Fprint(w, "cde")
		} else {
			fmt.Fprint(w, "abcdef")
		}
	}))
	defer server.Close()
	cfg := aws.Config{Region: "us-east-1", Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("test", "test", ""))}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
	store := &S3{client: client, bucket: "bucket"}
	got, err := store.GetRange(context.Background(), "data/test", 2, 3)
	if err != nil || string(got) != "cde" || requests != 1 {
		t.Fatalf("range read: %q, %v, requests=%d", got, err, requests)
	}
	serveRange = false
	if _, err := store.GetRange(context.Background(), "data/test", 2, 3); err == nil || requests != 2 {
		t.Fatalf("ignored range accepted or retried: %v, requests=%d", err, requests)
	}
}

func TestDeleteUsesPrefixAndSingleAttempt(t *testing.T) {
	requests := 0
	fail := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodDelete || r.URL.Path != "/bucket/db/data/dead" {
			t.Errorf("unexpected DELETE: %s %s", r.Method, r.URL.Path)
		}
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "<Error><Code>ServiceUnavailable</Code></Error>")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	cfg := aws.Config{Region: "us-east-1", Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("test", "test", ""))}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
	store := &S3{client: client, bucket: "bucket", prefix: "db/"}
	if err := store.Delete(context.Background(), "data/dead"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(context.Background(), "data/dead"); err != nil {
		t.Fatal(err)
	}
	fail = true
	if err := store.Delete(context.Background(), "data/dead"); err == nil {
		t.Fatal("delete failure ignored")
	}
	if requests != 3 {
		t.Fatalf("unexpected retry: %d requests", requests)
	}
}

func TestStatAndPaginatedInventoryUseNamespace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			if r.URL.Path != "/bucket/db/data/a" {
				t.Error(r.URL.Path)
			}
			w.Header().Set("Content-Length", "123")
			return
		}
		q := r.URL.Query()
		if q.Get("prefix") != "db/data/" || q.Get("max-keys") != "2" || q.Get("continuation-token") != "opaque token" {
			t.Errorf("inventory query: %v", q)
		}
		fmt.Fprint(w, `<ListBucketResult><Contents><Key>db/data/a</Key><Size>123</Size></Contents><NextContinuationToken>next</NextContinuationToken></ListBucketResult>`)
	}))
	defer server.Close()
	cfg := aws.Config{Region: "us-east-1", Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("test", "test", ""))}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
	store := &S3{client: client, bucket: "bucket", prefix: "db/"}
	size, err := store.Stat(context.Background(), "data/a")
	if err != nil || size != 123 {
		t.Fatalf("stat %d %v", size, err)
	}
	keys, next, err := store.List(context.Background(), "data/", "opaque token", 2)
	if err != nil || len(keys) != 1 || keys[0] != "data/a" || next != "next" {
		t.Fatalf("inventory %v %s %v", keys, next, err)
	}
}
