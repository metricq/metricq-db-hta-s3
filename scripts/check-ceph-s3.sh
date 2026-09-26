#!/usr/bin/env bash
set -euo pipefail

read -r -p 'S3-Bucket-URL (https://host/bucket): ' S3_CHECK_URL
read -r -p 'S3 Access Key ID: ' S3_CHECK_KEY
read -r -s -p 'S3 Secret Access Key: ' S3_CHECK_SECRET
printf '\n'

export S3_CHECK_URL S3_CHECK_KEY S3_CHECK_SECRET
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd -- "$script_dir/.."
go run ./cmd/check-ceph-s3
