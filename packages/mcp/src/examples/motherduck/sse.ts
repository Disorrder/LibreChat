import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import express from 'express';
import { createServer } from './motherduck.js';

const { server } = createServer();
let transport: SSEServerTransport;

// Add graceful shutdown handling
const shutdown = async () => {
  console.log('Shutting down gracefully...');
  if (transport) {
    await server.close();
  }
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

const app = express();

// @ts-ignore
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

app.get('/sse', async (req, res) => {
  console.log('New SSE connection');
  transport = new SSEServerTransport('/message', res);
  await server.connect(transport);

  res.on('close', async () => {
    console.log('SSE connection closed');
    await server.close();
  });
});

// @ts-ignore
app.post('/message', async (req, res) => {
  if (!transport) {
    return res.status(503).send('SSE connection not established');
  }
  await transport.handlePostMessage(req, res);
});

const PORT = process.env.PORT ?? 3008;
app.listen(PORT, () => {
  console.log(`MotherDuck MCP Server running on SSE at http://localhost:${PORT}/sse`);
});
