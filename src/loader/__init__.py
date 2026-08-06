"""Lands a date window of the committed Synthea export into the raw DuckDB schema.

The loader is the source of every distortion the warehouse is built to handle:
late-arriving claims, restatements, and mutating patient attributes. It also
writes the injection log recording what it did, which is the answer key the
pipeline is reconciled against, so nothing downstream may infer a distortion
that the log does not record.
"""
