package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type S3Config struct {
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix"`
	Endpoint  string `json:"endpoint"`
	Region    string `json:"region"`
	PathStyle bool   `json:"path_style"`
}
type S3 struct {
	client         *s3.Client
	bucket, prefix string
	identity       string
}

func NewS3(ctx context.Context, c S3Config) (*S3, error) {
	if c.Bucket == "" {
		return nil, fmt.Errorf("S3 bucket required")
	}
	if c.Endpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("invalid S3 endpoint; use AWS credentials separately")
		}
	}
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(c.Region))
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = c.PathStyle
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
		// Conditional manifest PUTs must be sent only once. The engine
		// reconciles an uncertain response by reading the manifest back.
		o.RetryMaxAttempts = 1
	})
	prefix := strings.Trim(c.Prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return &S3{client: client, bucket: c.Bucket, prefix: prefix, identity: c.Endpoint + "|" + c.Region + "|" + c.Bucket + "|" + prefix}, nil
}
func translate(err error) error {
	var e smithy.APIError
	if errors.As(err, &e) {
		switch e.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return fmt.Errorf("%w: %s", ErrNotFound, e.ErrorCode())
		case "PreconditionFailed", "ConditionalRequestConflict", "ConcurrentModification":
			return fmt.Errorf("%w: %s", ErrConflict, e.ErrorCode())
		}
	}
	return err
}
func (s *S3) Get(ctx context.Context, key string) ([]byte, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key)})
	if err != nil {
		return nil, "", translate(err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	return b, aws.ToString(out.ETag), err
}
func (s *S3) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if offset < 0 || length <= 0 || offset > int64(^uint64(0)>>1)-length {
		return nil, fmt.Errorf("invalid object range")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key), Range: &rangeHeader})
	if err != nil {
		return nil, translate(err)
	}
	defer out.Body.Close()
	// The total object size follows the slash; require the addressed range.
	if got := aws.ToString(out.ContentRange); !strings.HasPrefix(got, fmt.Sprintf("bytes %d-%d/", offset, offset+length-1)) {
		return nil, fmt.Errorf("object store did not honor byte range: %q", got)
	}
	b, err := io.ReadAll(io.LimitReader(out.Body, length+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != length {
		return nil, fmt.Errorf("short object range: got %d, want %d", len(b), length)
	}
	return b, nil
}
func (s *S3) Put(ctx context.Context, key string, b []byte, expected *string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	in := &s3.PutObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key), Body: bytes.NewReader(b)}
	if expected != nil {
		if *expected == "" {
			in.IfNoneMatch = aws.String("*")
		} else {
			// Ceph requires an unquoted If-Match even when GET returns a
			// quoted ETag. AWS S3 and MinIO accept this representation too.
			if len(*expected) >= 2 && (*expected)[0] == '"' && (*expected)[len(*expected)-1] == '"' {
				in.IfMatch = aws.String((*expected)[1 : len(*expected)-1])
			} else {
				in.IfMatch = expected
			}
		}
	}
	out, err := s.client.PutObject(ctx, in)
	if err != nil {
		return "", translate(err)
	}
	return aws.ToString(out.ETag), nil
}

func (s *S3) Identity() string { return s.identity }

func (s *S3) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + key)})
	if errors.Is(translate(err), ErrNotFound) {
		return nil
	}
	return translate(err)
}
