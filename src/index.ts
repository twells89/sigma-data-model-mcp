#!/usr/bin/env node
/**
 * Sigma Data Model MCP Server — stdio transport (local use).
 *
 * Usage with Claude Code:
 *   claude mcp add sigma-data-model -- node /path/to/build/index.js
 */

import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { createSigmaServer } from './tools.js';

async function main(): Promise<void> {
  const server = createSigmaServer();
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error('Sigma Data Model MCP Server running on stdio');
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
