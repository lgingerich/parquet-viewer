"""
Generates samples/example_v2.parquet (data pages v2) and samples/example_v1.parquet (data pages v1):
a file engineered to exercise as many Parquet features as possible, so the viewer has something
rich to show. Every column carries a short "why this exists" note which is also embedded in the
file's key-value metadata (key: parquet_viewer_guide) and shown in the viewer's "Example guide" tab.

Run (pyarrow is only needed to *generate* the sample, not to run the viewer):
    python3 -m venv /tmp/pqgen/venv && /tmp/pqgen/venv/bin/pip install pyarrow numpy
    /tmp/pqgen/venv/bin/python scripts/make_example.py
"""
import json
import os
import uuid
import hashlib
from decimal import Decimal

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

N = 18_000          # rows
ROW_GROUP = 5_000   # -> 4 row groups (5000, 5000, 5000, 3000)
rng = np.random.default_rng(42)
OUT = os.path.join(os.path.dirname(__file__), '..', 'samples')

WORDS = ['alpha', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'golf', 'hotel', 'india', 'juliet', 'kilo', 'lima',
         'mike', 'november', 'oscar', 'papa', 'quebec', 'romeo', 'sierra', 'tango', 'uniform', 'victor', 'whiskey']

# ---- column registry -------------------------------------------------------------------------
# Each entry: name, arrow array, (optional) compression, encoding, dictionary on/off, bloom filter, note
COLS = []


def col(name, array, note, *, codec=None, encoding=None, dictionary=None, bloom=None, nullable=True):
    """encoding: explicit column_encoding (disables dictionary). dictionary: force on/off (default: on unless encoding set)."""
    COLS.append(dict(name=name, array=array, note=note, codec=codec, encoding=encoding,
                     dictionary=(encoding is None) if dictionary is None else dictionary,
                     bloom=bloom, nullable=nullable))


def mask(p):
    return rng.random(N) < p


def with_nulls(values, p):
    m = mask(p)
    return [None if m[i] else v for i, v in enumerate(values)]


ids = np.cumsum(rng.integers(1, 6, N)) + 1_000

# --- integers
col('id', pa.array(ids, pa.int64()),
    'REQUIRED (non-null) sorted int64. DELTA_BINARY_PACKED stores tiny deltas, so it compresses hard; sorted data gives tight, '
    'non-overlapping min/max per row group (great for skipping). Also declared in sorting_columns and has a bloom filter. Codec: ZSTD.',
    codec='ZSTD', encoding='DELTA_BINARY_PACKED', bloom=dict(ndv=N, fpp=0.01), nullable=False)
col('rating', pa.array(with_nulls(rng.integers(1, 6, N).tolist(), 0.3), pa.int32()),
    'OPTIONAL int32 with ~30% nulls and only 5 distinct values. Shows definition levels, null_count in stats, and RLE_DICTIONARY '
    '(small dictionary page + bit-packed indexes).')
col('tiny_u8', pa.array(rng.integers(0, 256, N), pa.uint8()),
    'uint8 -> physical INT32 with logical INT(8, unsigned). Parquet has no small int types; logical types annotate the width.')
col('small_i16', pa.array(rng.integers(-30000, 30000, N), pa.int16()),
    'int16 -> physical INT32 with logical INT(16, signed).')
col('big_u64', pa.array(rng.integers(0, 2**62, N, dtype=np.uint64), pa.uint64()),
    'uint64 -> physical INT64 with logical INT(64, unsigned). Random values: dictionary overflows and falls back to PLAIN.',
    encoding='PLAIN')
col('always_null', pa.nulls(N, pa.int32()),
    'A column that is 100% null: null_count == num_values, and there is NO min/max. Data pages contain only definition levels.')

# --- strings / dictionary behaviour
col('category', pa.array(rng.choice(WORDS[:8], N)),
    'Low-cardinality string (8 values): textbook RLE_DICTIONARY. A DICTIONARY_PAGE comes first, then data pages of small integer indexes. Codec: SNAPPY.',
    codec='SNAPPY')
col('constant_region', pa.array(['us-east-1'] * N),
    'Every row identical: dictionary of ONE entry, indexes collapse to a single RLE run. Min == max. Codec: GZIP.',
    codec='GZIP')
col('session_token', pa.array([hashlib.md5(str(i).encode()).hexdigest() for i in range(N)]),
    'All-unique 32-char strings with dictionary ENABLED. The dictionary exceeds its size limit mid-chunk, so the writer FALLS BACK: '
    'look at the pages - dictionary-encoded data pages first, then PLAIN pages. Codec: LZ4_RAW.',
    codec='LZ4')
col('email', pa.array([f'user{i:05d}@example.com' for i in rng.permutation(N)]),
    'Unique strings with a bloom filter: lets readers prove a value is definitely absent without scanning. See the "bloom" tag and the bloom segment in the file map.',
    bloom=dict(ndv=N, fpp=0.01))
col('url_path', pa.array(sorted(f'/products/{WORDS[i % 6]}/{WORDS[(i // 6) % 6]}/item-{i:06d}' for i in range(N))),
    'Sorted strings sharing long prefixes: DELTA_BYTE_ARRAY (incremental/front coding) stores only prefix length + new suffix. Codec: BROTLI.',
    codec='BROTLI', encoding='DELTA_BYTE_ARRAY')
col('title', pa.array([' '.join(rng.choice(WORDS, rng.integers(1, 9))).title() for _ in range(N)]),
    'Variable-length strings: DELTA_LENGTH_BYTE_ARRAY stores all the lengths (delta-packed) first, then the raw bytes back-to-back. Codec: UNCOMPRESSED (ratio 1.00x).',
    codec='NONE', encoding='DELTA_LENGTH_BYTE_ARRAY')
col('huge_text', pa.array([None if rng.random() < 0.94 else 'The quick brown fox jumps over the lazy dog. ' * 94 + f'#{rng.integers(0, 10**6):06d}' for _ in range(N)]),
    'Mostly-null column of ~4.2 KB strings. Values exceed the writer\'s max statistics size (4 KiB), so min/max are OMITTED from the stats entirely '
    '(null_count survives) and no column index is written for it. Statistics are optional hints - readers must cope with them missing. Codec: ZSTD.',
    codec='ZSTD', encoding='PLAIN')

# --- floats
col('temperature', pa.array(rng.normal(15, 10, N), pa.float64()),
    'float64 with BYTE_STREAM_SPLIT: byte 0 of every value, then byte 1 of every value... Makes floats far more compressible. Codec: ZSTD.',
    codec='ZSTD', encoding='BYTE_STREAM_SPLIT')
col('score', pa.array(rng.random(N).astype(np.float32), pa.float32()),
    'float32 (physical FLOAT) with BYTE_STREAM_SPLIT.', encoding='BYTE_STREAM_SPLIT')
reading = rng.normal(0, 1, N)
reading[mask(0.02)] = np.nan
reading[mask(0.005)] = np.inf
col('reading_with_nan', pa.array(with_nulls(reading.tolist(), 0.05), pa.float64(), from_pandas=False),
    'float64 containing NaN, +inf and nulls. NaN is NOT null (it is excluded from min/max); inf is a normal ordered value; nulls are counted in null_count.',
    encoding='PLAIN')
col('half', pa.array(rng.random(N).astype(np.float16), pa.float16()),
    'float16 -> FIXED_LEN_BYTE_ARRAY(2) with logical FLOAT16.', encoding='PLAIN')

# --- booleans
runs = np.repeat(rng.integers(0, 2, N // 200 + 1).astype(bool), 200)[:N]
col('is_active', pa.array(runs),
    'Boolean in long runs: RLE encoding for booleans (run-length) instead of one bit per value.', encoding='RLE')
col('is_premium', pa.array(rng.random(N) < 0.3),
    'Random booleans: PLAIN encoding = bit-packed, 1 bit per value.', encoding='PLAIN')

# --- temporal
base = np.datetime64('2024-01-01T00:00:00', 'us')
col('created_at', pa.array(base + np.cumsum(rng.integers(1, 90_000_000, N)).astype('timedelta64[us]'), pa.timestamp('us', tz='UTC')),
    'timestamp[us, UTC]: physical INT64, logical TIMESTAMP(MICROS, isAdjustedToUTC=true). Monotonic, so DELTA_BINARY_PACKED.',
    encoding='DELTA_BINARY_PACKED')
col('local_ts', pa.array(base.astype('datetime64[ms]') + rng.integers(0, 10**10, N).astype('timedelta64[ms]'), pa.timestamp('ms')),
    'timestamp[ms] with NO timezone: same INT64 storage, but isAdjustedToUTC=false (a "wall clock" time).')
col('precise_ts', pa.array(base.astype('datetime64[ns]') + rng.integers(0, 10**15, N).astype('timedelta64[ns]'), pa.timestamp('ns', tz='UTC')),
    'timestamp[ns]: nanosecond precision (Parquet format 2.6+).')
col('birth_date', pa.array((np.datetime64('1950-01-01') + rng.integers(0, 25000, N)).astype('datetime64[D]'), pa.date32()),
    'date32 -> physical INT32 (days since epoch) with logical DATE.')
col('wake_time', pa.array(rng.integers(0, 86_400_000, N).astype(np.int32), pa.time32('ms')),
    'time32[ms] -> physical INT32, logical TIME(MILLIS).')

# --- decimals / binary / ids
def dec(scale, digits):
    # build big values from two int64 draws (numpy ints are capped at 63 bits)
    lo = min(digits, 12)
    return [Decimal(int(rng.integers(0, 10 ** (digits - lo + 1))) * 10 ** lo + int(rng.integers(0, 10 ** lo))).scaleb(-scale) for _ in range(N)]

col('price', pa.array(dec(2, 7), pa.decimal128(9, 2)),
    'decimal(9,2) -> stored as physical INT32 (precision <= 9). Logical type DECIMAL(precision, scale).')
col('amount', pa.array(dec(4, 15), pa.decimal128(18, 4)),
    'decimal(18,4) -> stored as physical INT64 (precision <= 18).')
col('big_decimal', pa.array(dec(10, 25), pa.decimal128(38, 10)),
    'decimal(38,10) -> too big for an integer, so FIXED_LEN_BYTE_ARRAY(16) holding big-endian two\'s complement.')
uuids = [uuid.UUID(int=int(rng.integers(0, 2**63)) << 64 | int(rng.integers(0, 2**63))).bytes for _ in range(N)]
col('uid', pa.ExtensionArray.from_storage(pa.uuid(), pa.array(uuids, pa.binary(16))),
    'UUID logical type on FIXED_LEN_BYTE_ARRAY(16). Bloom filter enabled.', bloom=dict(ndv=N, fpp=0.01))
col('payload', pa.array([rng.bytes(int(rng.integers(2, 24))) for _ in range(N)], pa.binary()),
    'Raw bytes (BYTE_ARRAY, no logical type) - no string/UTF-8 annotation, so min/max are unsigned byte comparisons.', encoding='PLAIN')
col('md5', pa.array([hashlib.md5(str(i).encode()).digest() for i in range(N)], pa.binary(16)),
    'fixed_size_binary(16) -> FIXED_LEN_BYTE_ARRAY(16), no logical type (type_length shown in the schema).', encoding='PLAIN')
col('json_doc', pa.array([json.dumps({'k': int(i), 'ok': bool(i % 2)}) for i in range(N)], pa.string()),
    'JSON text stored as a plain string column (a real JSON logical type exists too, but readers rarely use it).')

# --- nested
tags = []
for i in range(N):
    r = rng.random()
    tags.append(None if r < 0.1 else [] if r < 0.2 else [None if rng.random() < 0.1 else str(rng.choice(WORDS)) for _ in range(rng.integers(1, 5))])
col('tags', pa.array(tags, pa.list_(pa.string())),
    'list<string> with a NULL list, EMPTY lists and NULL elements: 3-level LIST encoding. Repetition levels mark where a new row starts; definition levels say how deep the value is non-null.')
col('attrs', pa.array([None if rng.random() < 0.1 else [(str(rng.choice(WORDS[:6])), int(rng.integers(0, 100))) for _ in range(rng.integers(0, 4))] for _ in range(N)],
                      pa.map_(pa.string(), pa.int32())),
    'map<string,int32>: stored as TWO column chunks (attrs.key_value.key and .value) sharing repetition levels.')
col('location', pa.array([None if rng.random() < 0.05 else {'lat': float(rng.uniform(-90, 90)), 'lon': float(rng.uniform(-180, 180)),
                          'address': None if rng.random() < 0.2 else {'city': str(rng.choice(WORDS)), 'zip': f'{rng.integers(0, 99999):05d}'}} for _ in range(N)],
                         pa.struct([('lat', pa.float64()), ('lon', pa.float64()),
                                    ('address', pa.struct([('city', pa.string()), ('zip', pa.string())]))])),
    'Nested struct (struct inside struct): each leaf (lat, lon, address.city, address.zip) is its own column chunk; nullability at each level is encoded in definition levels.')
col('matrix', pa.array([[list(map(int, rng.integers(0, 9, rng.integers(0, 4)))) for _ in range(rng.integers(0, 3))] for _ in range(N)], pa.list_(pa.list_(pa.int32()))),
    'list<list<int32>>: two levels of repetition (max repetition level = 2).')


# nested columns are stored as one chunk per *leaf*, addressed by dotted path
NESTED = {
    'tags.list.element':              ('ZSTD',   True),
    'attrs.key_value.key':            ('SNAPPY', True),
    'attrs.key_value.value':          ('GZIP',   True),
    'location.lat':                   ('ZSTD',   False),
    'location.lon':                   ('ZSTD',   False),
    'location.address.city':          ('SNAPPY', True),
    'location.address.zip':           ('GZIP',   False),
    'matrix.list.element.list.element': ('LZ4',  True),
}


def build(path, page_version):
    fields = [pa.field(c['name'], c['array'].type, nullable=c['nullable']) for c in COLS]
    guide = {c['name']: c['note'] for c in COLS}
    overview = (f'Synthetic example built to showcase Parquet features: {N:,} rows in row groups of {ROW_GROUP:,} (the last is smaller), '
                f'data page format {page_version}, page checksums, column and offset indexes, bloom filters, a declared sort order, and many codecs and encodings.')
    schema = pa.schema(fields, metadata={'parquet_viewer_overview': overview, 'parquet_viewer_guide': json.dumps(guide)})
    table = pa.table({c['name']: c['array'] for c in COLS}, schema=schema)
    codec = {c['name']: (c['codec'] or 'SNAPPY') for c in COLS}
    # leaf-name -> path for nested columns is handled by pyarrow; we only tune top-level flat columns.
    codec.update({k: v[0] for k, v in NESTED.items()})
    dict_cols = [c['name'] for c in COLS if c['dictionary']] + [k for k, v in NESTED.items() if v[1]]
    col_enc = {c['name']: c['encoding'] for c in COLS if c['encoding']}
    bloom = {c['name']: c['bloom'] for c in COLS if c['bloom']}
    with pq.ParquetWriter(
        path, schema, version='2.6', data_page_version=page_version,
        compression=codec, use_dictionary=dict_cols, column_encoding=col_enc,
        write_statistics=True, write_page_index=True, write_page_checksum=True,
        bloom_filter_options=bloom, sorting_columns=[pq.SortingColumn(0)],
        data_page_size=8 * 1024, dictionary_pagesize_limit=48 * 1024, write_batch_size=512,
        store_decimal_as_integer=True,
    ) as w:
        w.write_table(table, row_group_size=ROW_GROUP)


if __name__ == '__main__':
    os.makedirs(OUT, exist_ok=True)
    for name, ver in (('example_v2.parquet', '2.0'), ('example_v1.parquet', '1.0')):
        p = os.path.join(OUT, name)
        build(p, ver)
        print('wrote', os.path.normpath(p), os.path.getsize(p), 'bytes')
