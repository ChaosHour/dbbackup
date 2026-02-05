#!/bin/bash
# Release script for dbbackup
# Builds binaries and creates/updates GitHub release
#
# Usage:
#   ./release.sh              # Build and release current version
#   ./release.sh --bump       # Bump patch version, build, and release
#   ./release.sh --update     # Update existing release with new binaries
#   ./release.sh --dry-run    # Show what would happen without doing it

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# Configuration
TOKEN_FILE=".gh_token"
MAIN_FILE="main.go"

# Parse arguments
BUMP_VERSION=false
UPDATE_ONLY=false
DRY_RUN=false
RELEASE_MSG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --bump)
            BUMP_VERSION=true
            shift
            ;;
        --update)
            UPDATE_ONLY=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -m|--message)
            RELEASE_MSG="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --bump           Bump patch version before release"
            echo "  --update         Update existing release (don't create new)"
            echo "  --dry-run        Show what would happen without doing it"
            echo "  -m, --message    Release message/comment (required for new releases)"
            echo "  --help           Show this help"
            echo ""
            echo "Examples:"
            echo "  $0 -m \"Fix TUI crash on cluster restore\""
            echo "  $0 --bump -m \"Add new backup compression option\""
            echo "  $0 --update  # Just update binaries, no message needed"
            echo ""
            echo "Token file: .gh_token (gitignored)"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage"
            exit 1
            ;;
    esac
done

# Check for GitHub token
if [ ! -f "$TOKEN_FILE" ]; then
    echo -e "${RED}❌ Token file not found: $TOKEN_FILE${NC}"
    echo ""
    echo "Create it with:"
    echo "  echo 'your_github_token' > $TOKEN_FILE"
    echo ""
    echo "The file is gitignored for security."
    exit 1
fi

GH_TOKEN=$(cat "$TOKEN_FILE" | tr -d '[:space:]')
if [ -z "$GH_TOKEN" ]; then
    echo -e "${RED}❌ Token file is empty${NC}"
    exit 1
fi

export GH_TOKEN

# Get current version
CURRENT_VERSION=$(grep 'version.*=' "$MAIN_FILE" | head -1 | sed 's/.*"\(.*\)".*/\1/')
echo -e "${BLUE}📦 Current version: ${YELLOW}${CURRENT_VERSION}${NC}"

# Bump version if requested
if [ "$BUMP_VERSION" = true ]; then
    # Parse version (X.Y.Z)
    MAJOR=$(echo "$CURRENT_VERSION" | cut -d. -f1)
    MINOR=$(echo "$CURRENT_VERSION" | cut -d. -f2)
    PATCH=$(echo "$CURRENT_VERSION" | cut -d. -f3)
    
    NEW_PATCH=$((PATCH + 1))
    NEW_VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"
    
    echo -e "${GREEN}📈 Bumping version: ${YELLOW}${CURRENT_VERSION}${NC} → ${GREEN}${NEW_VERSION}${NC}"
    
    if [ "$DRY_RUN" = false ]; then
        sed -i "s/version.*=.*\"${CURRENT_VERSION}\"/version   = \"${NEW_VERSION}\"/" "$MAIN_FILE"
        CURRENT_VERSION="$NEW_VERSION"
    fi
fi

TAG="v${CURRENT_VERSION}"
echo -e "${BLUE}🏷️  Release tag: ${YELLOW}${TAG}${NC}"

# Require message for new releases (not updates)
if [ -z "$RELEASE_MSG" ] && [ "$UPDATE_ONLY" = false ] && [ "$DRY_RUN" = false ]; then
    echo -e "${RED}❌ Release message required. Use -m \"Your message\"${NC}"
    echo ""
    echo "Example:"
    echo "  $0 -m \"Fix TUI crash on cluster restore\""
    exit 1
fi

if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}🔍 DRY RUN - No changes will be made${NC}"
    echo ""
    echo "Would execute:"
    echo "  1. Build binaries with build_all.sh"
    echo "  2. Commit and push changes"
    echo "  3. Create/update release ${TAG}"
    exit 0
fi

# Build binaries
echo ""
echo -e "${BOLD}${BLUE}🔨 Building binaries...${NC}"
bash build_all.sh

# Check if there are changes to commit
if [ -n "$(git status --porcelain)" ]; then
    echo ""
    echo -e "${BLUE}📝 Committing changes...${NC}"
    git add -A
    
    # Generate commit message using the release message
    if [ -n "$RELEASE_MSG" ]; then
        COMMIT_MSG="${TAG}: ${RELEASE_MSG}"
    elif [ "$BUMP_VERSION" = true ]; then
        COMMIT_MSG="${TAG}: Version bump"
    else
        COMMIT_MSG="${TAG}: Release build"
    fi
    
    git commit -m "$COMMIT_MSG"
fi

# Push changes
echo -e "${BLUE}⬆️  Pushing to origin...${NC}"
git push origin main

# Handle tag
TAG_EXISTS=$(git tag -l "$TAG")
if [ -z "$TAG_EXISTS" ]; then
    echo -e "${BLUE}🏷️  Creating tag ${TAG}...${NC}"
    git tag "$TAG"
    git push origin "$TAG"
else
    echo -e "${YELLOW}⚠️  Tag ${TAG} already exists${NC}"
fi

# Check if release exists
echo ""
echo -e "${BLUE}🚀 Preparing release...${NC}"

RELEASE_EXISTS=$(gh release view "$TAG" 2>/dev/null && echo "yes" || echo "no")

if [ "$RELEASE_EXISTS" = "yes" ] || [ "$UPDATE_ONLY" = true ]; then
    echo -e "${YELLOW}📦 Updating existing release ${TAG}...${NC}"
    
    # Delete existing assets and upload new ones
    for binary in bin/dbbackup_*; do
        if [ -f "$binary" ]; then
            ASSET_NAME=$(basename "$binary")
            echo "  Uploading $ASSET_NAME..."
            gh release upload "$TAG" "$binary" --clobber
        fi
    done
else
    echo -e "${GREEN}📦 Creating new release ${TAG}...${NC}"
    
    # Generate release notes with the provided message
    NOTES="## ${TAG}: ${RELEASE_MSG}

### Downloads
| Platform | Architecture | Binary |
|----------|--------------|--------|
| Linux | x86_64 (Intel/AMD) | \`dbbackup_linux_amd64\` |
| Linux | ARM64 | \`dbbackup_linux_arm64\` |
| Linux | ARMv7 | \`dbbackup_linux_arm_armv7\` |
| macOS | Intel | \`dbbackup_darwin_amd64\` |
| macOS | Apple Silicon (M1/M2) | \`dbbackup_darwin_arm64\` |

### Installation
\`\`\`bash
# Linux x86_64
curl -LO https://github.com/PlusOne/dbbackup/releases/download/${TAG}/dbbackup_linux_amd64
chmod +x dbbackup_linux_amd64
sudo mv dbbackup_linux_amd64 /usr/local/bin/dbbackup

# macOS Apple Silicon
curl -LO https://github.com/PlusOne/dbbackup/releases/download/${TAG}/dbbackup_darwin_arm64
chmod +x dbbackup_darwin_arm64
sudo mv dbbackup_darwin_arm64 /usr/local/bin/dbbackup
\`\`\`
"

    gh release create "$TAG" \
        --title "${TAG}: ${RELEASE_MSG}" \
        --notes "$NOTES" \
        bin/dbbackup_linux_amd64 \
        bin/dbbackup_linux_arm64 \
        bin/dbbackup_linux_arm_armv7 \
        bin/dbbackup_darwin_amd64 \
        bin/dbbackup_darwin_arm64
fi

echo ""
echo -e "${GREEN}${BOLD}✅ Release complete!${NC}"
echo -e "   ${BLUE}https://github.com/PlusOne/dbbackup/releases/tag/${TAG}${NC}"
