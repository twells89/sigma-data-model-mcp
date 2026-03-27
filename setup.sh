#!/bin/bash
# Setup script for Sigma Data Model MCP Server
set -e

echo "📦 Installing dependencies..."
npm install

echo "🔨 Building TypeScript..."
npm run build

echo ""
echo "✅ Build complete!"
echo ""
echo "════════════════════════════════════════════════"
echo "  REMOTE (hosted) — for sharing with others"
echo "════════════════════════════════════════════════"
echo ""
echo "  Start the HTTP server:"
echo "    npm start"
echo ""
echo "  Deploy to Render: push to GitHub, then connect"
echo "  the repo at https://dashboard.render.com"
echo ""
echo "  Once deployed, others connect with:"
echo "    claude mcp add sigma-data-model --transport http https://YOUR-HOST/mcp"
echo ""
echo "════════════════════════════════════════════════"
echo "  LOCAL (stdio) — for personal use"
echo "════════════════════════════════════════════════"
echo ""
echo "  claude mcp add sigma-data-model -- node $(pwd)/build/index.js"
echo ""
