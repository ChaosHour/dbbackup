# Fallback instructions for release 85

If you need to hard reset to the last known good release (v3.42.85):

1. Fetch the tag from remote:
   git fetch --tags

2. Checkout the release tag:
   git checkout v3.42.85

3. (Optional) Hard reset main to this tag:
   git checkout main
   git reset --hard v3.42.85
   git push --force origin main
   git push --force github main

4. Re-run CI to verify stability.

# Note
- This will revert all changes after v3.42.85.
- Only use if CI and builds are broken and cannot be fixed quickly.
