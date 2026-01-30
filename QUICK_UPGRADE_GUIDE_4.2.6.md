# dbbackup v4.2.6 Quick Reference Card

## 🔥 WHAT CHANGED

### CRITICAL SECURITY FIXES
1. **Password flag removed** - Was: `--password` → Now: `PGPASSWORD` env var
2. **Backup files secured** - Was: 0644 (world-readable) → Now: 0600 (owner-only)
3. **Race conditions fixed** - Parallel backups now stable

## 🚀 MIGRATION (2 MINUTES)

### Before (v4.2.5)
```bash
dbbackup backup --password=secret --host=localhost
```

### After (v4.2.6) - Choose ONE:

**Option 1: Environment Variable (Recommended)**
```bash
export PGPASSWORD=secret       # PostgreSQL
export MYSQL_PWD=secret        # MySQL
dbbackup backup --host=localhost
```

**Option 2: Config File**
```bash
echo "password: secret" >> ~/.dbbackup/config.yaml
dbbackup backup --host=localhost
```

**Option 3: PostgreSQL .pgpass**
```bash
echo "localhost:5432:*:postgres:secret" >> ~/.pgpass
chmod 0600 ~/.pgpass
dbbackup backup --host=localhost
```

## ✅ VERIFY SECURITY

### Test 1: Password Not in Process List
```bash
dbbackup backup &
ps aux | grep dbbackup
# ✅ Should NOT see password
```

### Test 2: Backup Files Secured
```bash
dbbackup backup
ls -l /backups/*.tar.gz
# ✅ Should see: -rw------- (0600)
```

## 📦 INSTALL

```bash
# Linux (amd64)
wget https://github.com/YOUR_ORG/dbbackup/releases/download/v4.2.6/dbbackup_linux_amd64
chmod +x dbbackup_linux_amd64
sudo mv dbbackup_linux_amd64 /usr/local/bin/dbbackup

# Verify
dbbackup --version
# Should output: dbbackup version 4.2.6
```

## 🎯 WHO NEEDS TO UPGRADE

| Environment | Priority | Upgrade By |
|-------------|----------|------------|
| Multi-user production | **CRITICAL** | Immediately |
| Single-user production | **HIGH** | 24 hours |
| Development | **MEDIUM** | This week |
| Testing | **LOW** | At convenience |

## 📞 NEED HELP?

- **Security Issues:** Email maintainers (private)
- **Bug Reports:** GitHub Issues
- **Questions:** GitHub Discussions
- **Docs:** docs/ directory

## 🔗 LINKS

- **Full Release Notes:** RELEASE_NOTES_4.2.6.md
- **Changelog:** CHANGELOG.md
- **Expert Feedback:** EXPERT_FEEDBACK_SIMULATION.md

---

**Version:** 4.2.6  
**Status:** ✅ Production Ready  
**Build Date:** 2026-01-30  
**Commit:** fd989f4
