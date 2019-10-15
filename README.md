# Cloud storage notifications for Go

Watch objects in S3 buckets and get notifiations when they have changed.

Inspired by [fsnotify](https://github.com/fsnotify/fsnotify)

## Notes

* Specify path in S3: `s3://<bucketName>/path/to/object`.
* Only supports objects.

## Examples

* **s3-reload** - https://github.com/app-sre/s3-reload
