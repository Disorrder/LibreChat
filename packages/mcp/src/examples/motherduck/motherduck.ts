import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  ListPromptsRequestSchema,
  GetPromptRequestSchema,
  ListResourcesRequestSchema,
  ReadResourceRequestSchema,
  TextContent,
} from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { zodToJsonSchema } from 'zod-to-json-schema';
import duckdb, { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';

console.log(duckdb.version());
// console.log(duckdb.configurationOptionDescriptions());

const SERVER_VERSION = '0.2.2';

// Import the prompt template from a separate file
import { PROMPT_TEMPLATE } from './prompt.js';

// Define input schemas using Zod
const InitializeConnectionSchema = z.object({
  type: z.string().describe('Type of the database, either "DuckDB" or "MotherDuck"'),
});

const ReadSchemasSchema = z.object({
  database_name: z.string().describe('name of the database'),
});

const ExecuteQuerySchema = z.object({
  query: z.string().describe('SQL query to execute'),
});

let instance: DuckDBInstance | null = null;
let connection: DuckDBConnection | null = null;

export const createServer = () => {
  console.log('🚀 ~ createServer ~ instance:', instance);

  const server = new Server(
    {
      name: 'mcp-server-motherduck',
      version: SERVER_VERSION,
    },
    {
      capabilities: {
        tools: {},
        prompts: {},
        resources: {},
      },
    },
  );

  // Resource handlers
  server.setRequestHandler(ListResourcesRequestSchema, async () => {
    return { resources: [] };
  });

  server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
    throw new Error(`Unsupported URI scheme: ${request.params.uri}`);
  });

  // Prompt handlers
  server.setRequestHandler(ListPromptsRequestSchema, async () => {
    return {
      prompts: [
        {
          name: 'duckdb-motherduck-initial-prompt',
          description:
            'A prompt to initialize a connection to duckdb or motherduck and start working with it',
        },
      ],
    };
  });

  server.setRequestHandler(GetPromptRequestSchema, async (request) => {
    const { name } = request.params;
    if (name !== 'duckdb-motherduck-initial-prompt') {
      throw new Error(`Unknown prompt: ${name}`);
    }

    return {
      description: 'Initial prompt for interacting with DuckDB/MotherDuck',
      messages: [
        {
          role: 'user',
          content: {
            type: 'text',
            text: PROMPT_TEMPLATE,
          },
        },
      ],
    };
  });

  // Tool handlers
  server.setRequestHandler(ListToolsRequestSchema, async () => {
    return {
      tools: [
        {
          name: 'initialize-connection',
          description:
            'Create a connection to either a local DuckDB or MotherDuck and retrieve available databases',
          inputSchema: zodToJsonSchema(InitializeConnectionSchema),
        },
        {
          name: 'read-schemas',
          description: 'Get table schemas from a specific DuckDB/MotherDuck database',
          inputSchema: zodToJsonSchema(ReadSchemasSchema),
        },
        {
          name: 'execute-query',
          description: 'Execute a query on the MotherDuck (DuckDB) database',
          inputSchema: zodToJsonSchema(ExecuteQuerySchema),
        },
      ],
    };
  });

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;
    console.log('🚀 ~ server.setRequestHandler ~ name:', name, args);

    switch (name) {
      case 'initialize-connection': {
        const { type } = InitializeConnectionSchema.parse(args);
        const dbType = type.trim().toUpperCase();

        if (!['DUCKDB', 'MOTHERDUCK'].includes(dbType)) {
          throw new Error('Only "DuckDB" or "MotherDuck" are supported');
        }

        if (dbType === 'MOTHERDUCK' && !process.env.motherduck_token) {
          throw new Error('Please set the `motherduck_token` environment variable.');
        }

        try {
          if (!instance) {
            instance = await DuckDBInstance.create(':memory:');
          }
          connection = await instance.connect();

          const databases = await connection.run(`
            SELECT string_agg(database_name, ',\n')
            FROM duckdb_databases() 
            WHERE database_name NOT IN ('system', 'temp')
          `);

          return {
            content: [
              {
                type: 'text',
                text: `Connection to ${dbType} successfully established. Here are the available databases: \n${databases}`,
              } as TextContent,
            ],
          };
        } catch (error) {
          // @ts-expect-error error is not typed
          throw new Error(`Failed to connect: ${error.message}`);
        }
      }

      case 'read-schemas': {
        if (!connection) {
          throw new Error('No active database connection. Please initialize one first.');
        }

        const { database_name } = ReadSchemasSchema.parse(args);

        try {
          const tables = await connection.run(`
            SELECT string_agg(regexp_replace(sql, 'CREATE TABLE ', 'CREATE TABLE '||database_name||'.'), '\n\n') as sql 
            FROM duckdb_tables()
            WHERE database_name = '${database_name}'
          `);

          const views = await connection.run(`
            SELECT string_agg(regexp_replace(sql, 'CREATE TABLE ', 'CREATE TABLE '||database_name||'.'), '\n\n') as sql 
            FROM duckdb_views()
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'localmemdb')
            AND view_name NOT IN (
              'duckdb_columns','duckdb_constraints','duckdb_databases','duckdb_indexes',
              'duckdb_schemas','duckdb_tables','duckdb_types','duckdb_views','pragma_database_list',
              'sqlite_master','sqlite_schema','sqlite_temp_master','sqlite_temp_schema'
            )
            AND database_name = '${database_name}'
          `);

          return {
            content: [
              {
                type: 'text',
                text: `Here are all tables: \n${tables} \n\n Here are all views: ${views}`,
              } as TextContent,
            ],
          };
        } catch (error) {
          // @ts-expect-error error is not typed
          throw new Error(`Failed to read schemas: ${error.message}`);
        }
      }

      case 'execute-query': {
        if (!connection) {
          throw new Error('No active database connection. Please initialize one first.');
        }

        const { query } = ExecuteQuerySchema.parse(args);

        try {
          const results = await connection.run(query);
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify(results, null, 2),
              } as TextContent,
            ],
          };
        } catch (error) {
          // @ts-expect-error error is not typed
          throw new Error(`Error querying the database: ${error.message}`);
        }
      }

      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  });

  return { server };
};
