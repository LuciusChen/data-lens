# 014 — Unibyte Buffer Required for Binary Protocol I/O

## Background

The MySQL wire protocol uses byte values above 127 as sentinel markers:
- `0xFE` (254): EOF packet marker (also used for large row counts)
- `0xFF` (255): ERR packet marker

`mysql.el` accumulated incoming data in a standard Emacs buffer created with
`generate-new-buffer`. Emacs buffers default to multibyte mode, where bytes in
the range 128–255 are stored as multi-byte sequences. The process filter used
`insert` to write raw binary data into this multibyte buffer, silently
corrupting every byte ≥ 128.

## Root Cause

`(set-process-coding-system proc 'binary 'binary)` was set on the process —
which controls how bytes are decoded *before* reaching the filter function —
but the filter's `insert` call then re-encoded them into the multibyte buffer
representation. The result: `0xFE` and `0xFF` were stored as two-byte
sequences, making `(aref buf pos)` return the wrong byte value.
Packet-type detection (`mysql--packet-type`) read corrupted byte values instead
of the actual markers, misrouting entire packets.

The bug surfaced inconsistently because most responses (OK, column definitions,
row data) consist of ASCII-range bytes. It only triggered on specific values:
auth challenge salts containing high bytes, column counts above 127 in wide
result sets, or server version strings with non-ASCII characters.

## Fix

After `generate-new-buffer`, immediately call `(set-buffer-multibyte nil)` on
the input buffer before attaching it to the process. This makes `insert` store
bytes verbatim. All subsequent `aref` / `substring` / `length` calls operate on
the raw byte sequence, matching the protocol's binary framing exactly.

The `pg.el` input buffer received the same fix.

## Additional Changes in the Same Commit

- **Connection error during auth**: `mysql-connection-error` thrown while
  reading the auth-response packet is re-signaled as `mysql-auth-error` with a
  clearer message. Previously a generic "connection closed" appeared for bad
  passwords.
- **OS output flush before giving up**: when `accept-process-output` returns
  nil and the process is dead, a 50ms drain attempts to flush any buffered OS
  data before declaring a connection error.
- **Default buf field**: changed `mysql-conn` `buf` default from a
  pre-allocated buffer (created at `defstruct` expansion time, before
  `mysql-connect` runs) to `nil`. The pre-allocated buffer was never used and
  leaked on every file load.

## Lesson

Any Emacs buffer that receives binary process output must be marked unibyte at
creation time with `(set-buffer-multibyte nil)`. `set-process-coding-system
'binary` is not sufficient — it controls the OS/Emacs codec boundary, not the
buffer's character representation. The process filter's `insert` call is subject
to the buffer's own multibyte flag. These are two independent encoding layers
and both must be configured for raw binary I/O.
