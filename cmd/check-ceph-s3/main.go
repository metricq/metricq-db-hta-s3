// Command check-ceph-s3 probes whether an S3 endpoint supports the operations
// the database needs: create-only and If-Match conditional PUT, read-after-
// write consistency and deletion. It creates the bucket if needed and writes
// and deletes one random object. Configure it with S3_CHECK_URL
// (https://host/bucket), S3_CHECK_KEY, S3_CHECK_SECRET and optionally
// S3_CHECK_REGION.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

func bucketURL(raw string) (string, string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", "", fmt.Errorf("URL must be https://host/bucket (or http://host/bucket)")
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 1 || parts[0] == "" {
		return "", "", fmt.Errorf("URL must contain exactly one bucket path component")
	}
	bucket, err := url.PathUnescape(parts[0])
	if err != nil || bucket == "" || strings.Contains(bucket, "/") {
		return "", "", fmt.Errorf("invalid bucket name in URL")
	}
	u.Path, u.RawPath = "", ""
	return u.String(), bucket, nil
}

func isConflict(err error) bool {
	var api smithy.APIError
	if !errors.As(err, &api) {
		return false
	}
	switch api.ErrorCode() {
	case "PreconditionFailed", "ConditionalRequestConflict", "ConcurrentModification":
		return true
	default:
		return false
	}
}

func ensureBucket(ctx context.Context, client *s3.Client, bucket string) (bool, error) {
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucket}); err == nil {
		fmt.Printf("Bucket s3://%s ist vorhanden\n", bucket)
		return false, nil
	}
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket}); err == nil {
		fmt.Printf("Bucket s3://%s wurde angelegt und bleibt bestehen\n", bucket)
		return true, nil
	} else {
		var api smithy.APIError
		if errors.As(err, &api) && api.ErrorCode() == "BucketAlreadyOwnedByYou" {
			fmt.Printf("Bucket s3://%s ist vorhanden\n", bucket)
			return false, nil
		}
		return false, fmt.Errorf("Bucket s3://%s prüfen/anlegen: %w", bucket, err)
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("PASS: getestete bedingte PUTs und Read-after-Write funktionieren")
}

func run() error {
	endpoint, bucket, err := bucketURL(os.Getenv("S3_CHECK_URL"))
	if err != nil {
		return err
	}
	access, secret := os.Getenv("S3_CHECK_KEY"), os.Getenv("S3_CHECK_SECRET")
	if access == "" || secret == "" {
		return fmt.Errorf("Access Key ID und Secret Access Key sind erforderlich")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	region := os.Getenv("S3_CHECK_REGION")
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(access, secret, "")))
	if err != nil {
		return err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1 // Preserve the response to each conditional request.
	})
	if _, err := ensureBucket(ctx, client, bucket); err != nil {
		return err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	key := "metricq-s3-check/" + hex.EncodeToString(random[:])
	fmt.Printf("Testobjekt: s3://%s/%s\n", bucket, key)
	var versionMu sync.Mutex
	var versions []string
	created := false
	defer func() {
		if created {
			cleanupCtx, done := context.WithTimeout(context.Background(), 15*time.Second)
			defer done()
			if len(versions) == 0 {
				if _, err := client.DeleteObject(cleanupCtx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil {
					fmt.Fprintf(os.Stderr, "Testobjekt konnte nicht entfernt werden: %v\n", err)
				}
			} else {
				for _, version := range versions {
					if _, err := client.DeleteObject(cleanupCtx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key, VersionId: &version}); err != nil {
						fmt.Fprintf(os.Stderr, "Testobjekt-Version %s konnte nicht entfernt werden: %v\n", version, err)
					}
				}
			}
		}
	}()
	put := func(body, match, noMatch string) (string, error) {
		in := &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader([]byte(body))}
		if match != "" {
			in.IfMatch = &match
		}
		if noMatch != "" {
			in.IfNoneMatch = &noMatch
		}
		out, err := client.PutObject(ctx, in)
		if err != nil {
			return "", err
		}
		if out.VersionId != nil {
			versionMu.Lock()
			versions = append(versions, *out.VersionId)
			versionMu.Unlock()
		}
		return aws.ToString(out.ETag), nil
	}
	get := func(want string) (string, error) {
		out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
		if err != nil {
			return "", err
		}
		defer out.Body.Close()
		b, err := io.ReadAll(out.Body)
		if err != nil {
			return "", err
		}
		if string(b) != want || aws.ToString(out.ETag) == "" {
			return "", fmt.Errorf("GET liefert unerwarteten Inhalt oder kein ETag")
		}
		return aws.ToString(out.ETag), nil
	}
	putETag, err := put("initial", "", "*")
	if err != nil {
		return fmt.Errorf("erstes If-None-Match: * PUT: %w", err)
	}
	created = true
	fmt.Println("OK: neues Objekt mit If-None-Match: * angelegt")
	etag, err := get("initial")
	if err != nil {
		return fmt.Errorf("GET direkt nach PUT: %w", err)
	}
	fmt.Printf("Diagnose-Etag: PutObject=%q, GetObject=%q\n", putETag, etag)
	if _, err := put("must-not-overwrite", "", "*"); !isConflict(err) {
		return fmt.Errorf("zweites If-None-Match: * PUT muss mit 412/409 scheitern, erhielt: %v", err)
	}
	if _, err := get("initial"); err != nil {
		return fmt.Errorf("Objekt nach abgelehntem PUT verändert: %w", err)
	}
	fmt.Println("OK: If-None-Match verhindert Überschreiben")
	if _, err := put("must-not-overwrite", "\"wrong-etag\"", ""); !isConflict(err) {
		return fmt.Errorf("falsches If-Match muss mit 412/409 scheitern, erhielt: %v", err)
	}
	// Match the production adapter: a conditional PUT uses exactly one
	// unquoted ETag. A conflict is reported, never retried with another format.
	formatIfMatch := func(value string) string {
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			return value[1 : len(value)-1]
		}
		return value
	}
	if _, err := put("updated", formatIfMatch(etag), ""); err != nil {
		return fmt.Errorf("If-Match mit unquoted ETag: %w", err)
	}
	etag, err = get("updated")
	if err != nil {
		return fmt.Errorf("GET direkt nach bedingtem PUT: %w", err)
	}
	fmt.Println("OK: If-Match und Read-after-Write")
	casETag := formatIfMatch(etag)
	for round := 1; round <= 10; round++ {
		type result struct {
			value string
			etag  string
			err   error
		}
		results := make(chan result, 2)
		var start sync.WaitGroup
		start.Add(2)
		for _, value := range []string{fmt.Sprintf("writer-a-%d", round), fmt.Sprintf("writer-b-%d", round)} {
			go func(value string) {
				start.Done()
				start.Wait()
				newETag, err := put(value, casETag, "")
				results <- result{value, newETag, err}
			}(value)
		}
		a, b := <-results, <-results
		var winner result
		if a.err == nil && isConflict(b.err) {
			winner = a
		} else if b.err == nil && isConflict(a.err) {
			winner = b
		} else {
			return fmt.Errorf("Runde %d: erwartet genau einen erfolgreichen If-Match PUT und einen 412/409; erhielt %v und %v", round, a.err, b.err)
		}
		etag, err = get(winner.value)
		if err != nil {
			return fmt.Errorf("Runde %d: veröffentlichter Inhalt: %w", round, err)
		}
		if winner.etag != "" && winner.etag != etag {
			return fmt.Errorf("Runde %d: ETag aus PUT (%q) weicht von GET (%q) ab", round, winner.etag, etag)
		}
		casETag = formatIfMatch(etag)
	}
	fmt.Println("OK: zehn konkurrierende If-Match-Paare, jeweils genau ein Gewinner")
	return nil
}
