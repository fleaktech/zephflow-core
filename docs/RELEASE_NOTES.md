# Release Notes

## Breaking changes

### `gcs_source` removed

The standalone `gcs_source` command has been removed. Use `fs_source` with
`backend: gs` instead. One-line config migration:

    # Before — REMOVED
    gcs_source:
      bucket: my-bucket
      prefix: data/
      encodingType: JSON

    # After
    fs_source:
      backend: gs
      root: gs://my-bucket/data/
      emission: { type: WHOLE_FILE, encoding: utf-8 }
      mode: BOUNDED
      partition: { index: 0, parallelism: 1 }
