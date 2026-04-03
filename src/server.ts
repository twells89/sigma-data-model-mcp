#!/usr/bin/env node
/**
 * Sigma Data Model MCP Server — Streamable HTTP transport (remote hosting).
 *
 * This is a stateless server: each request gets its own MCP server instance.
 * All converter tools are pure transformations with no user state needed,
 * so stateless mode is the simplest and most scalable approach.
 *
 * Deploy to Render, Railway, Koyeb, or any Node.js host.
 *
 * Usage:
 *   node build/server.js                  # starts on PORT (default 3000)
 *
 * Clients connect to:
 *   https://your-host.onrender.com/mcp
 */

import express from 'express';
import type { Request, Response } from 'express';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { createSigmaServer } from './tools.js';

const app = express();
app.use(express.json());

// ── Health check (for Render, load balancers, uptime monitors) ───────────────

app.get('/', (_req: Request, res: Response) => {
  res.json({
    name: 'sigma-data-model-mcp',
    version: '1.0.0',
    description: 'MCP server for converting dbt, Snowflake, LookML, Tableau, Power BI, and Omni Analytics data models to Sigma Computing format',
    endpoint: '/mcp',
    transport: 'streamable-http',
    tools: [
      'convert_dbt_to_sigma',
      'convert_snowflake_to_sigma',
      'convert_lookml_to_sigma',
      'convert_powerbi_to_sigma',
      'convert_tableau_to_sigma',
      'convert_omni_to_sigma',
      'convert_sql_to_sigma_formula',
      'convert_tableau_formula_to_sigma',
      'parse_lookml',
      'get_sigma_data_model_schema',
      'format_sigma_display_name',
    ],
  });
});

// ── MCP Streamable HTTP endpoint (stateless mode) ────────────────────────────
//
// Stateless = each POST creates a fresh McpServer + transport. No sessions.
// This is ideal because all our tools are pure functions: YAML in → JSON out.
// No user state, no auth, no side effects.

async function handleMcp(req: Request, res: Response): Promise<void> {
  try {
    const server = createSigmaServer();
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined, // stateless — no sessions
    });

    res.on('close', () => {
      transport.close();
      server.close();
    });

    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (error) {
    console.error('Error handling MCP request:', error);
    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: '2.0',
        error: { code: -32603, message: 'Internal server error' },
        id: null,
      });
    }
  }
}

app.post('/mcp', handleMcp);
app.get('/mcp', handleMcp);
app.delete('/mcp', handleMcp);

// ── Start ────────────────────────────────────────────────────────────────────

const PORT = parseInt(process.env.PORT || '3000', 10);
app.listen(PORT, () => {
  console.log(`Sigma Data Model MCP Server listening on port ${PORT}`);
  console.log(`MCP endpoint: http://localhost:${PORT}/mcp`);
  console.log(`Health check: http://localhost:${PORT}/`);
});
