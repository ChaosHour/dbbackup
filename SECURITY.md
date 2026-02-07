# Security Policy

## Supported Versions

We release security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 5.7.x   | :white_check_mark: |
| 5.6.x   | :white_check_mark: |
| 5.5.x   | :white_check_mark: |
| < 5.5   | :x:                |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

### Preferred Method: Private Disclosure

**Email:** security@uuxo.net

**Include in your report:**
1. **Description** - Clear description of the vulnerability
2. **Impact** - What an attacker could achieve
3. **Reproduction** - Step-by-step instructions to reproduce
4. **Version** - Affected dbbackup version(s)
5. **Environment** - OS, database type, configuration
6. **Proof of Concept** - Code or commands demonstrating the issue (if applicable)

### Response Timeline

- **Initial Response:** Within 48 hours
- **Status Update:** Within 7 days
- **Fix Timeline:** Depends on severity
  - **Critical:** 1-3 days
  - **High:** 1-2 weeks
  - **Medium:** 2-4 weeks
  - **Low:** Next release cycle

### Severity Levels

**Critical:**
- Remote code execution
- SQL injection
- Arbitrary file read/write
- Authentication bypass
- Encryption key exposure

**High:**
- Privilege escalation
- Information disclosure (sensitive data)
- Denial of service (easily exploitable)

**Medium:**
- Information disclosure (non-sensitive)
- Denial of service (requires complex conditions)
- CSRF attacks

**Low:**
- Information disclosure (minimal impact)
- Issues requiring local access

## Security Best Practices

### For Users

**Encryption Keys:**
- - RECOMMENDED: Generate strong 32-byte keys: `head -c 32 /dev/urandom | base64 > key.file`
- - RECOMMENDED: Store keys securely (KMS, HSM, or encrypted filesystem)
- - RECOMMENDED: Use unique keys per environment
- - AVOID: Never commit keys to version control
- - AVOID: Never share keys over unencrypted channels

**Database Credentials:**
- - RECOMMENDED: Use read-only accounts for backups when possible
- - RECOMMENDED: Rotate credentials regularly
- - RECOMMENDED: Use environment variables or secure config files
- - AVOID: Never hardcode credentials in scripts
- - AVOID: Avoid using root/admin accounts

**Backup Storage:**
- - RECOMMENDED: Encrypt backups with `--encrypt` flag
- - RECOMMENDED: Use secure cloud storage with encryption at rest
- - RECOMMENDED: Implement proper access controls (IAM, ACLs)
- - RECOMMENDED: Enable backup retention and versioning
- - AVOID: Never store unencrypted backups on public storage

**Docker Usage:**
- - RECOMMENDED: Use specific version tags (`:v3.2.0` not `:latest`)
- - RECOMMENDED: Run as non-root user (default in our image)
- - RECOMMENDED: Mount volumes read-only when possible
- - RECOMMENDED: Use Docker secrets for credentials
- - AVOID: Don't run with `--privileged` unless necessary

### For Developers

**Code Security:**
- Always validate user input
- Use parameterized queries (no SQL injection)
- Sanitize file paths (no directory traversal)
- Handle errors securely (no sensitive data in logs)
- Use crypto/rand for random generation

**Dependencies:**
- Keep dependencies updated
- Review security advisories for Go packages
- Use `go mod verify` to check integrity
- Scan for vulnerabilities with `govulncheck`

**Secrets in Code:**
- Never commit secrets to git
- Use `.gitignore` for sensitive files
- Rotate any accidentally exposed credentials
- Use environment variables for configuration

## Known Security Considerations

### Encryption

**AES-256-GCM:**
- Uses authenticated encryption (prevents tampering)
- PBKDF2 with 600,000 iterations (OWASP 2023 recommendation)
- Unique nonce per encryption operation
- Secure random generation (crypto/rand)

**Key Management:**
- Keys are NOT stored by dbbackup
- Users responsible for key storage and management
- Support for multiple key sources (file, env, passphrase)

### Database Access

**Credential Handling:**
- Credentials passed via environment variables
- Connection strings support sslmode/ssl options
- Support for certificate-based authentication

**Network Security:**
- Supports SSL/TLS for database connections
- No credential caching or persistence
- Connections closed immediately after use

### Cloud Storage

**Cloud Provider Security:**
- Uses official SDKs (AWS, Azure, Google)
- Supports IAM roles and managed identities
- Respects provider encryption settings
- No credential storage (uses provider auth)

## Security Audit History

| Date       | Auditor          | Scope                    | Status |
|------------|------------------|--------------------------|--------|
| 2025-11-26 | Internal Review  | Initial release audit    | - RECOMMENDED: Pass |

## Vulnerability Disclosure Policy

**Coordinated Disclosure:**
1. Reporter submits vulnerability privately
2. We confirm and assess severity
3. We develop and test a fix
4. We prepare security advisory
5. We release patched version
6. We publish security advisory
7. Reporter receives credit (if desired)

**Public Disclosure:**
- Security advisories published after fix is available
- CVE requested for critical/high severity issues
- Credit given to reporter (unless anonymity requested)

## Security Updates

**Notification Channels:**
- Security advisories on repository
- Release notes for patched versions
- Email notification (for enterprise users)

**Updating:**
```bash
# Check current version
./dbbackup --version

# Download latest version
wget https://git.uuxo.net/PlusOne/dbbackup/releases/latest

# Or pull latest Docker image
docker pull git.uuxo.net/PlusOne/dbbackup:latest
```

## Contact

**Security Issues:** security@uuxo.net  
**General Issues:** https://git.uuxo.net/PlusOne/dbbackup/issues  
**Repository:** https://git.uuxo.net/PlusOne/dbbackup

---

**We take security seriously and appreciate responsible disclosure.**

Thank you for helping keep dbbackup and its users safe!
