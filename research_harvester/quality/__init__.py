"""Quality isolation layer for harvested research results (stdlib only).

Reads the collector's landing tables (``raw_pages`` / ``result_versions``),
applies a versioned quality contract, and maintains an atomically published
projection that downstream statistics read through pointer views.
"""
