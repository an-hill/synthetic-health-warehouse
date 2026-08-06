"""Lands a date window of the committed Synthea export into the raw DuckDB schema.

Most of what the warehouse has to cope with is already in the export: claims fan
out over encounters and are billed days or weeks after the service. What the
export lacks, the loader injects and records in the injection log, which is the
answer key the pipeline is reconciled against, so nothing downstream may infer a
distortion that the log does not record.
"""
