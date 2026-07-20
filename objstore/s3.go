package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// S3Store implements ObjectStore backed by Amazon S3.
type S3Store struct {
	client *s3.Client
	bucket string
	prefix string
}

// S3Config holds configuration for creating an S3Store.
type S3Config struct {
	Bucket          string
	Region          string
	Endpoint        string
	Prefix          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// R2Config holds configuration for creating a Cloudflare R2-backed store.
type R2Config struct {
	AccountID       string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Prefix          string
	Jurisdiction    string
	Endpoint        string
}

// NewS3Store creates a new S3-backed object store.
func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.AccessKeyID != "" || cfg.SecretAccessKey != "" || cfg.SessionToken != "" {
		if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
			return nil, fmt.Errorf("both AccessKeyID and SecretAccessKey are required for static credentials")
		}
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	s3Opts = append(s3Opts, func(o *s3.Options) {
		// Avoid automatic checksums unless an operation requires them. Not all
		// S3-compatible stores support the same checksum algorithms, and the SDK
		// otherwise spends time warning/retrying.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)
	return &S3Store{
		client: client,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
	}, nil
}

// NewR2Store creates a new Cloudflare R2-backed object store.
func NewR2Store(ctx context.Context, cfg R2Config) (*S3Store, error) {
	endpoint := cfg.Endpoint
	if endpoint == "" {
		if cfg.AccountID == "" {
			return nil, fmt.Errorf("AccountID is required when Endpoint is not set")
		}

		jurisdiction := strings.ToLower(strings.TrimSpace(cfg.Jurisdiction))
		switch jurisdiction {
		case "", "default":
			endpoint = fmt.Sprintf("https://%s.r2.cloudflarestorage.com", cfg.AccountID)
		default:
			endpoint = fmt.Sprintf("https://%s.%s.r2.cloudflarestorage.com", cfg.AccountID, jurisdiction)
		}
	}

	return NewS3Store(ctx, S3Config{
		Bucket:          cfg.Bucket,
		Region:          "auto",
		Endpoint:        endpoint,
		Prefix:          cfg.Prefix,
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		SessionToken:    cfg.SessionToken,
	})
}

func (s *S3Store) fullKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

func (s *S3Store) Put(ctx context.Context, key string, data []byte, ifNoneMatch bool) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
		Body:   bytes.NewReader(data),
	}

	if ifNoneMatch {
		input.IfNoneMatch = aws.String("*")
	}

	_, err := s.client.PutObject(ctx, input)
	if err != nil {
		// Check for conditional write failure (HTTP 412).
		var respErr *smithyhttp.ResponseError
		if errors.As(err, &respErr) && respErr.HTTPStatusCode() == 412 {
			return ErrPreconditionFailed
		}
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	return nil
}

func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer output.Body.Close()
	return io.ReadAll(output.Body)
}

func (s *S3Store) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	rangeStr := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil, fmt.Errorf("s3 get range %s: %w", key, err)
	}
	defer output.Body.Close()
	return io.ReadAll(output.Body)
}

func (s *S3Store) Head(ctx context.Context, key string) (*ObjectMeta, error) {
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		var nf *types.NotFound
		if errors.As(err, &nf) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil, fmt.Errorf("s3 head %s: %w", key, err)
	}

	meta := &ObjectMeta{
		Key: key,
	}
	if output.ContentLength != nil {
		meta.Size = *output.ContentLength
	}
	if output.ETag != nil {
		meta.ETag = *output.ETag
	}
	return meta, nil
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		return fmt.Errorf("s3 delete %s: %w", key, err)
	}
	return nil
}

func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := s.fullKey(prefix)
	var keys []string

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			// Strip the store prefix to return relative keys.
			k := *obj.Key
			if s.prefix != "" {
				k = k[len(s.prefix)+1:]
			}
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (s *S3Store) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.Head(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
