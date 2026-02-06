# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.0.x   | Yes       |

## Reporting a Vulnerability

If you discover a security vulnerability in MicroRedis, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, please email: **mateuszsury** (via GitHub profile contact)

### What to include

- Description of the vulnerability
- Steps to reproduce
- Impact assessment
- Suggested fix (if any)

### Response timeline

- **Acknowledgment**: within 48 hours
- **Initial assessment**: within 1 week
- **Fix release**: as soon as practical, depending on severity

## Security Considerations

MicroRedis is designed for **embedded/IoT environments** on trusted local networks. Keep the following in mind:

### Authentication

- MicroRedis supports `AUTH` password-based authentication via the `requirepass` config option
- Passwords are transmitted in plaintext (RESP2 protocol does not support TLS natively)
- Always use AUTH when exposing MicroRedis on any network

### Network

- MicroRedis binds to `0.0.0.0` by default -- restrict this to specific interfaces in production
- There is no TLS/SSL support -- do not expose MicroRedis to untrusted networks without a TLS proxy
- Rate limiting middleware is available to mitigate abuse

### Data

- MicroRedis stores all data in RAM -- data is lost on power loss unless persistence is configured
- Snapshot files (`.mrdb`) are not encrypted -- protect them at the filesystem level
- The `KEYS *` command scans all keys and may be slow with many keys -- avoid in production

### Platform

- ESP32-S3 does not have hardware memory protection -- any code running on the device has full access
- MicroPython does not provide sandboxing or isolation between modules
