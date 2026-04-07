#!/usr/bin/env node

/**
 * Austrian Financial Regulation MCP — stdio entry point.
 *
 * Provides MCP tools for querying FMA (Finanzmarktaufsicht) regulatory documents:
 * Rundschreiben, Mindeststandards, Leitfaeden, and enforcement actions.
 *
 * Tool prefix: at_fin_
 * Primary language: German
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import {
  listSourcebooks,
  searchProvisions,
  getProvision,
  searchEnforcement,
  checkProvisionCurrency,
} from "./db.js";
import { buildCitation } from './citation.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let pkgVersion = "0.1.0";
try {
  const pkg = JSON.parse(
    readFileSync(join(__dirname, "..", "package.json"), "utf8"),
  ) as { version: string };
  pkgVersion = pkg.version;
} catch {
  // fallback to default
}

const SERVER_NAME = "austrian-financial-regulation-mcp";

// --- Tool definitions ---

const TOOLS = [
  {
    name: "at_fin_search_regulations",
    description:
      "Volltextsuche in FMA-Regelwerken: Rundschreiben, Mindeststandards und Leitfaeden. Liefert passende Anforderungen, Hinweise und Standards. Suchanfragen auf Deutsch empfohlen.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Suchanfrage auf Deutsch (z.B. 'IT-Sicherheit', 'Risikomanagement', 'Auslagerung')",
        },
        sourcebook: {
          type: "string",
          description: "Filter nach Quellenbereich-ID (z.B. FMA_Rundschreiben, FMA_Mindeststandards). Optional.",
        },
        status: {
          type: "string",
          enum: ["in_force", "deleted", "not_yet_in_force"],
          description: "Filter nach Status der Bestimmung. Standardmaessig alle Status.",
        },
        limit: {
          type: "number",
          description: "Maximale Anzahl der Ergebnisse. Standard: 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "at_fin_get_regulation",
    description:
      "Liefert eine spezifische FMA-Bestimmung nach Quellenbereich und Referenz (z.B. 'FMA_Rundschreiben RS-IT-2020').",
    inputSchema: {
      type: "object" as const,
      properties: {
        sourcebook: {
          type: "string",
          description: "Quellenbereich-ID (z.B. FMA_Rundschreiben, FMA_Mindeststandards, FMA_Leitfaeden)",
        },
        reference: {
          type: "string",
          description: "Vollstaendige Referenz der Bestimmung (z.B. 'RS-IT-2020 Abschnitt 3')",
        },
      },
      required: ["sourcebook", "reference"],
    },
  },
  {
    name: "at_fin_list_sourcebooks",
    description:
      "Listet alle verfuegbaren FMA-Quellenbereiche mit Namen und Beschreibungen.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "at_fin_search_enforcement",
    description:
      "Suche in FMA-Durchsetzungsmassnahmen: Bescheide, Strafen, Lizenzentzug und Verwarnungen. Suchanfragen auf Deutsch empfohlen.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Suchanfrage (z.B. Firmenname, Art des Verstosses, 'Geldwaesche')",
        },
        action_type: {
          type: "string",
          enum: ["fine", "ban", "restriction", "warning"],
          description: "Filter nach Massnahmentyp. Optional.",
        },
        limit: {
          type: "number",
          description: "Maximale Anzahl der Ergebnisse. Standard: 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "at_fin_check_currency",
    description:
      "Prueft, ob eine spezifische FMA-Bestimmung derzeit in Kraft ist. Gibt Status und Gueltigkeitsdatum zurueck.",
    inputSchema: {
      type: "object" as const,
      properties: {
        reference: {
          type: "string",
          description: "Vollstaendige Referenz der zu pruefenden Bestimmung",
        },
      },
      required: ["reference"],
    },
  },
  {
    name: "at_fin_about",
    description: "Gibt Metadaten ueber diesen MCP-Server zurueck: Version, Datenquelle, Werkzeugliste.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
];

// --- Zod schemas for argument validation ---

const SearchRegulationsArgs = z.object({
  query: z.string().min(1),
  sourcebook: z.string().optional(),
  status: z.enum(["in_force", "deleted", "not_yet_in_force"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetRegulationArgs = z.object({
  sourcebook: z.string().min(1),
  reference: z.string().min(1),
});

const SearchEnforcementArgs = z.object({
  query: z.string().min(1),
  action_type: z.enum(["fine", "ban", "restriction", "warning"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const CheckCurrencyArgs = z.object({
  reference: z.string().min(1),
});

// --- Helper ---

function textContent(data: unknown) {
  return {
    content: [
      { type: "text" as const, text: JSON.stringify(data, null, 2) },
    ],
  };
}

function errorContent(message: string) {
  return {
    content: [{ type: "text" as const, text: message }],
    isError: true as const,
  };
}

// --- Server setup ---

const server = new Server(
  { name: SERVER_NAME, version: pkgVersion },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;

  try {
    switch (name) {
      case "at_fin_search_regulations": {
        const parsed = SearchRegulationsArgs.parse(args);
        const results = searchProvisions({
          query: parsed.query,
          sourcebook: parsed.sourcebook,
          status: parsed.status,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "at_fin_get_regulation": {
        const parsed = GetRegulationArgs.parse(args);
        const provision = getProvision(parsed.sourcebook, parsed.reference);
        if (!provision) {
          return errorContent(
            "Bestimmung nicht gefunden: " + parsed.sourcebook + " " + parsed.reference,
          );
        }
        return textContent({
          ...(typeof provision === 'object' ? provision : { data: provision }),
          _citation: buildCitation(
            provision.reference || parsed.reference,
            provision.title || provision.name || parsed.reference,
            'at_fin_get_regulation',
            { sourcebook: parsed.sourcebook, reference: parsed.reference },
            provision.url || provision.source_url || null,
          ),
        });
      }

      case "at_fin_list_sourcebooks": {
        const sourcebooks = listSourcebooks();
        return textContent({ sourcebooks, count: sourcebooks.length });
      }

      case "at_fin_search_enforcement": {
        const parsed = SearchEnforcementArgs.parse(args);
        const results = searchEnforcement({
          query: parsed.query,
          action_type: parsed.action_type,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "at_fin_check_currency": {
        const parsed = CheckCurrencyArgs.parse(args);
        const currency = checkProvisionCurrency(parsed.reference);
        return textContent(currency);
      }

      case "at_fin_about": {
        return textContent({
          name: SERVER_NAME,
          version: pkgVersion,
          description:
            "FMA (Oesterreichische Finanzmarktaufsicht) MCP-Server. Bietet Zugang zu FMA-Rundschreiben, Mindeststandards, Leitfaeden und Durchsetzungsmassnahmen.",
          data_source: "FMA Regelwerk (https://www.fma.gv.at/)",
          tools: TOOLS.map((t) => ({ name: t.name, description: t.description })),
        });
      }

      default:
        return errorContent("Unbekanntes Werkzeug: " + name);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return errorContent("Fehler bei der Ausfuehrung von " + name + ": " + message);
  }
});

// --- Main ---

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write(SERVER_NAME + " v" + pkgVersion + " running on stdio\n");
}

main().catch((err) => {
  process.stderr.write("Fatal error: " + (err instanceof Error ? err.message : String(err)) + "\n");
  process.exit(1);
});
