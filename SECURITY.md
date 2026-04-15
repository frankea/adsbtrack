# Security Policy

## Reporting a vulnerability

If you discover a security vulnerability in adsbtrack, please report it by [opening an issue](https://github.com/frankea/adsbtrack/issues/new) on GitHub.

For sensitive issues that should not be disclosed publicly, use [GitHub's private vulnerability reporting](https://github.com/frankea/adsbtrack/security/advisories/new) if available.

## Scope

adsbtrack is a CLI tool that processes publicly available ADS-B data. It does not run as a network service and does not handle authentication credentials beyond optional API keys stored in local config files.

Security concerns most relevant to this project:

- Command injection via user-supplied input (hex codes, URLs, file paths)
- SQL injection in SQLite queries
- Credential leakage (API keys in credentials.json)
- Dependency vulnerabilities
