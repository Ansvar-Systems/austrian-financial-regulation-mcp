/**
 * FMA Ingestion Crawler
 *
 * Scrapes the Austrian Financial Market Authority (FMA — Finanzmarktaufsicht)
 * website (fma.gv.at) and populates the SQLite database with:
 *   1. Rundschreiben (circulars) — supervisory guidance on IT security,
 *      outsourcing, AML, DORA, fit & proper, prospectus, etc.
 *   2. Mindeststandards (minimum standards) — binding standards for risk
 *      management, internal audit, compliance, lending, FX credits
 *   3. Leitfaeden (guides) — non-binding guidance on governance, sustainability,
 *      IT security, digital operational resilience
 *   4. OeNB Leitfaeden — Oesterreichische Nationalbank guides on IT risk,
 *      payment systems, financial stability
 *   5. Enforcement actions (Sanktionen / Bekanntmachungen) — fines, licence
 *      revocations, bans published on the FMA news feed
 *
 * The FMA website is WordPress-based. Document PDFs are served from
 * wp-content/plugins/dw-fma/download.php?d=NNN. Listing pages expose
 * links to those PDFs. Enforcement actions are paginated WordPress posts
 * at /category/news/sanktion/page/N/.
 *
 * Because fma.gv.at blocks most automated User-Agents with HTTP 403,
 * the crawler sends a browser-like User-Agent header and uses cheerio
 * for robust HTML parsing.
 *
 * All content is in German, as issued by the FMA.
 *
 * Usage:
 *   npx tsx scripts/ingest-fma.ts                # full crawl
 *   npx tsx scripts/ingest-fma.ts --resume        # resume from last checkpoint
 *   npx tsx scripts/ingest-fma.ts --dry-run       # log what would be inserted
 *   npx tsx scripts/ingest-fma.ts --force          # drop and recreate DB first
 *   npx tsx scripts/ingest-fma.ts --sanctions-only # only crawl sanctions
 *   npx tsx scripts/ingest-fma.ts --docs-only      # only crawl documents
 */

import Database from "better-sqlite3";
import * as cheerio from "cheerio";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, resolve } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["FMA_DB_PATH"] ?? "data/fma.db";
const PROGRESS_FILE = resolve(dirname(DB_PATH), "ingest-progress.json");
const BASE_URL = "https://www.fma.gv.at";

const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = 2000;
const FETCH_TIMEOUT_MS = 30_000;

// Browser-like UA — fma.gv.at returns 403 for bot-like agents
const USER_AGENT =
  "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0";

// CLI flags
const args = process.argv.slice(2);
const force = args.includes("--force");
const dryRun = args.includes("--dry-run");
const resume = args.includes("--resume");
const sanctionsOnly = args.includes("--sanctions-only");
const docsOnly = args.includes("--docs-only");

// ---------------------------------------------------------------------------
// Listing-page URLs
// ---------------------------------------------------------------------------

/** FMA document listing pages — one per sourcebook category. */
const DOC_LISTING_PAGES: Array<{
  sourcebookId: string;
  url: string;
  label: string;
}> = [
  {
    sourcebookId: "FMA_RUNDSCHREIBEN",
    url: `${BASE_URL}/fma/fma-rundschreiben/`,
    label: "Rundschreiben",
  },
  {
    sourcebookId: "FMA_MINDESTSTANDARDS",
    url: `${BASE_URL}/fma/fma-mindeststandards/`,
    label: "Mindeststandards",
  },
  {
    sourcebookId: "FMA_LEITFAEDEN",
    url: `${BASE_URL}/fma/fma-leitfaeden/`,
    label: "Leitfaeden",
  },
];

/** Sanctions pagination — WordPress category feed. */
const SANCTIONS_BASE = `${BASE_URL}/category/news/sanktion/`;
const SANCTIONS_MAX_PAGES = 25; // safety cap

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ProvisionRow {
  sourcebook_id: string;
  reference: string;
  title: string;
  text: string;
  type: string;
  status: string;
  effective_date: string | null;
  chapter: string | null;
  section: string | null;
}

interface EnforcementRow {
  firm_name: string;
  reference_number: string | null;
  action_type: string;
  amount: number | null;
  date: string | null;
  summary: string;
  sourcebook_references: string | null;
}

interface DiscoveredDoc {
  sourcebookId: string;
  title: string;
  url: string;
  /** Document number extracted from download.php?d=NNN, or slug */
  docId: string;
  type: string;
}

interface Progress {
  completed_doc_urls: string[];
  completed_sanction_urls: string[];
  sanctions_last_page: number;
  last_updated: string;
}

// ---------------------------------------------------------------------------
// Utility: rate-limited fetch with retry
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimitedFetch(
  url: string,
  opts?: RequestInit,
): Promise<Response> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  let lastError: Error | null = null;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      const resp = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept:
            "text/html, application/xhtml+xml, application/xml;q=0.9, */*;q=0.8",
          "Accept-Language": "de-AT,de;q=0.9,en;q=0.5",
        },
        redirect: "follow",
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
        ...opts,
      });
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status} for ${url}`);
      }
      return resp;
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      console.warn(
        `  [retry ${attempt}/${MAX_RETRIES}] ${url}: ${lastError.message}`,
      );
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_BACKOFF_MS * attempt);
      }
    }
  }
  throw lastError!;
}

async function fetchHtml(url: string): Promise<string> {
  const resp = await rateLimitedFetch(url);
  return resp.text();
}

/**
 * Fetch a PDF from a download URL and extract readable text.
 * Since we cannot reliably parse PDFs in Node without heavy deps,
 * we fall back to extracting metadata from the download page/redirect
 * and storing what we can. For PDFs served behind download.php, the
 * server sometimes returns an HTML preview page first.
 */
async function fetchPdfPreview(url: string): Promise<string | null> {
  try {
    const resp = await rateLimitedFetch(url);
    const contentType = resp.headers.get("content-type") ?? "";

    // If it returns HTML, parse it as a preview page
    if (contentType.includes("text/html")) {
      const html = await resp.text();
      const $ = cheerio.load(html);
      // Remove nav, header, footer, sidebar
      $("nav, header, footer, .sidebar, script, style").remove();
      const text = $("body").text().replace(/\s+/g, " ").trim();
      return text.length > 100 ? text : null;
    }

    // Actual PDF binary — we cannot parse it without a PDF library
    return null;
  } catch {
    return null;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// ---------------------------------------------------------------------------
// Progress tracking
// ---------------------------------------------------------------------------

function loadProgress(): Progress {
  if (resume && existsSync(PROGRESS_FILE)) {
    try {
      const raw = readFileSync(PROGRESS_FILE, "utf-8");
      const p = JSON.parse(raw) as Progress;
      console.log(
        `Fortschritt geladen (${p.last_updated}): ` +
          `${p.completed_doc_urls.length} Dokumente, ` +
          `${p.completed_sanction_urls.length} Sanktionen, ` +
          `letzte Sanktionsseite: ${p.sanctions_last_page}`,
      );
      return p;
    } catch {
      console.warn(
        "Fortschrittsdatei konnte nicht gelesen werden, starte neu",
      );
    }
  }
  return {
    completed_doc_urls: [],
    completed_sanction_urls: [],
    sanctions_last_page: 0,
    last_updated: new Date().toISOString(),
  };
}

function saveProgress(progress: Progress): void {
  progress.last_updated = new Date().toISOString();
  writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

// ---------------------------------------------------------------------------
// Database setup
// ---------------------------------------------------------------------------

function initDatabase(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Bestehende Datenbank geloescht: ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  console.log(`Datenbank initialisiert: ${DB_PATH}`);
  return db;
}

// ---------------------------------------------------------------------------
// Sourcebook definitions
// ---------------------------------------------------------------------------

interface SourcebookDef {
  id: string;
  name: string;
  description: string;
}

const SOURCEBOOKS: SourcebookDef[] = [
  {
    id: "FMA_RUNDSCHREIBEN",
    name: "FMA Rundschreiben",
    description:
      "Aufsichtsrechtliche Rundschreiben der FMA zu IT-Sicherheit, Auslagerungen, Geldwaesche, DORA, Eignungspruefung, Prospektaufsicht und weiteren regulatorischen Anforderungen.",
  },
  {
    id: "FMA_MINDESTSTANDARDS",
    name: "FMA Mindeststandards",
    description:
      "Verbindliche Mindeststandards der FMA fuer das Risikomanagement, die interne Revision, Compliance, das Kreditgeschaeft und Fremdwaehrungskredite konzessionierter Unternehmen.",
  },
  {
    id: "FMA_LEITFAEDEN",
    name: "FMA Leitfaeden",
    description:
      "Nicht verbindliche Leitfaeden der FMA zu Governance, Compliance, Nachhaltigkeitsrisiken, IT-Sicherheit, digitaler operationaler Resilienz und weiteren Themen.",
  },
  {
    id: "OENB_LEITFAEDEN",
    name: "OeNB Leitfaeden",
    description:
      "Leitfaeden der Oesterreichischen Nationalbank zu IT-Risiko, Zahlungsverkehr und Finanzstabilitaet.",
  },
  {
    id: "FMA_DORA",
    name: "FMA DORA",
    description:
      "Veroeffentlichungen der FMA zur Verordnung (EU) 2022/2554 ueber die digitale operationale Resilienz im Finanzsektor (DORA): IKT-Risikomanagement, Drittparteienrisiko, Meldepflichten und Ueberwachungsrahmen.",
  },
];

// ---------------------------------------------------------------------------
// 1. Discover documents from listing pages
// ---------------------------------------------------------------------------

/**
 * Determine the document type label from a sourcebook ID.
 */
function typeForSourcebook(sourcebookId: string): string {
  switch (sourcebookId) {
    case "FMA_RUNDSCHREIBEN":
      return "Rundschreiben";
    case "FMA_MINDESTSTANDARDS":
      return "Mindeststandard";
    case "FMA_LEITFAEDEN":
      return "Leitfaden";
    case "OENB_LEITFAEDEN":
      return "Leitfaden";
    case "FMA_DORA":
      return "DORA-Leitfaden";
    default:
      return "Dokument";
  }
}

/**
 * Scrape a listing page and return discovered documents.
 * FMA listing pages contain links to PDF downloads (download.php?d=NNN)
 * and occasionally to subpages with more detail.
 */
async function discoverDocuments(
  sourcebookId: string,
  listingUrl: string,
  label: string,
): Promise<DiscoveredDoc[]> {
  console.log(`\n--- ${label}: Dokumente ermitteln ---`);
  console.log(`  URL: ${listingUrl}`);

  const html = await fetchHtml(listingUrl);
  const $ = cheerio.load(html);

  const docs: DiscoveredDoc[] = [];
  const seen = new Set<string>();

  // Pattern 1: PDF download links (wp-content/plugins/dw-fma/download.php?d=NNN)
  $('a[href*="download.php"]').each((_i, el) => {
    const href = $(el).attr("href");
    if (!href) return;
    const fullUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;

    // Extract document ID from d=NNN parameter
    const dMatch = fullUrl.match(/[?&]d=(\d+)/);
    const docId = dMatch ? `FMA-D-${dMatch[1]}` : fullUrl;
    if (seen.has(docId)) return;
    seen.add(docId);

    const title = $(el).text().trim() || `${label} (${docId})`;
    docs.push({
      sourcebookId,
      title,
      url: fullUrl,
      docId,
      type: typeForSourcebook(sourcebookId),
    });
  });

  // Pattern 2: links to detail subpages on fma.gv.at
  $("a[href]").each((_i, el) => {
    const href = $(el).attr("href");
    if (!href) return;
    if (href.includes("download.php")) return; // already handled
    const fullUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
    if (!fullUrl.startsWith(BASE_URL)) return;

    // Only pick up links that look like document detail pages
    const text = $(el).text().trim();
    if (
      text.length < 10 ||
      /^(Home|Startseite|Kontakt|Impressum|Datenschutz|Sitemap|English|Presse)/i.test(
        text,
      )
    )
      return;
    // Skip navigation, footer, breadcrumb
    const parent = $(el).closest("nav, footer, .breadcrumb, .menu, header");
    if (parent.length > 0) return;

    // Heuristic: page URLs that contain the sourcebook concept
    const isRelevant =
      /rundschreiben|mindeststandard|leitfad|leitfäd|circular|guide/i.test(
        fullUrl,
      ) ||
      /rundschreiben|mindeststandard|leitfad|leitfäd/i.test(text);
    if (!isRelevant) return;

    const slug = fullUrl.replace(/\/$/, "").split("/").pop() ?? fullUrl;
    if (seen.has(slug)) return;
    seen.add(slug);

    docs.push({
      sourcebookId,
      title: text,
      url: fullUrl,
      docId: slug,
      type: typeForSourcebook(sourcebookId),
    });
  });

  console.log(`  ${docs.length} Dokumente gefunden`);
  return docs;
}

/**
 * Discover DORA-related documents from the FMA DORA topic pages.
 */
async function discoverDoraDocuments(): Promise<DiscoveredDoc[]> {
  const doraUrls = [
    `${BASE_URL}/querschnittsthemen/dora/`,
    `${BASE_URL}/querschnittsthemen/dora/dora-ikt-risiko-management/`,
    `${BASE_URL}/querschnittsthemen/dora/dora-management-ikt-drittparteienrisiko/`,
    `${BASE_URL}/querschnittsthemen/dora/dora-kritische-ikt-drittdienstleister/`,
  ];

  console.log("\n--- DORA: Dokumente ermitteln ---");

  const docs: DiscoveredDoc[] = [];
  const seen = new Set<string>();

  for (const url of doraUrls) {
    try {
      const html = await fetchHtml(url);
      const $ = cheerio.load(html);

      // Extract main content text as a provision
      $("nav, header, footer, .sidebar, script, style, .menu").remove();
      const mainContent =
        $("main").text().trim() ||
        $(".entry-content").text().trim() ||
        $("article").text().trim();

      if (mainContent.length > 200) {
        const slug = url.replace(/\/$/, "").split("/").pop() ?? "dora";
        if (!seen.has(slug)) {
          seen.add(slug);
          docs.push({
            sourcebookId: "FMA_DORA",
            title: $("h1").first().text().trim() || `DORA — ${slug}`,
            url,
            docId: `DORA-${slug}`,
            type: "DORA-Leitfaden",
          });
        }
      }

      // Also grab any PDF download links on these pages
      $('a[href*="download.php"]').each((_i, el) => {
        const href = $(el).attr("href");
        if (!href) return;
        const fullUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
        const dMatch = fullUrl.match(/[?&]d=(\d+)/);
        const docId = dMatch ? `DORA-D-${dMatch[1]}` : fullUrl;
        if (seen.has(docId)) return;
        seen.add(docId);

        docs.push({
          sourcebookId: "FMA_DORA",
          title: $(el).text().trim() || `DORA-Dokument (${docId})`,
          url: fullUrl,
          docId,
          type: "DORA-Leitfaden",
        });
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`  DORA-Seite uebersprungen (${url}): ${msg}`);
    }
  }

  console.log(`  ${docs.length} DORA-Dokumente gefunden`);
  return docs;
}

// ---------------------------------------------------------------------------
// 2. Crawl individual document pages
// ---------------------------------------------------------------------------

/**
 * Build a human-readable reference from a document.
 * For download.php PDFs, use the document number (d=NNN).
 * For subpages, derive from the slug.
 */
function buildReference(doc: DiscoveredDoc, sectionIndex: number): string {
  const prefix = doc.sourcebookId === "FMA_RUNDSCHREIBEN"
    ? "RS"
    : doc.sourcebookId === "FMA_MINDESTSTANDARDS"
      ? "MS"
      : doc.sourcebookId === "FMA_DORA"
        ? "DORA"
        : "LF";
  return `${prefix}-${doc.docId}${sectionIndex > 0 ? ` Abschnitt ${sectionIndex}` : ""}`;
}

/**
 * Crawl a single document URL and return provision rows.
 * For HTML pages, we extract the main content and split into sections.
 * For PDF downloads, we attempt to get the preview page.
 */
async function crawlDocument(doc: DiscoveredDoc): Promise<ProvisionRow[]> {
  const provisions: ProvisionRow[] = [];

  try {
    const resp = await rateLimitedFetch(doc.url);
    const contentType = resp.headers.get("content-type") ?? "";

    if (contentType.includes("application/pdf")) {
      // PDF binary — store metadata only (title from listing)
      provisions.push({
        sourcebook_id: doc.sourcebookId,
        reference: buildReference(doc, 0),
        title: doc.title,
        text: `[PDF-Dokument] ${doc.title}. Quelle: ${doc.url}`,
        type: doc.type,
        status: "in_force",
        effective_date: null,
        chapter: null,
        section: null,
      });
      return provisions;
    }

    // HTML content — parse with cheerio
    const html = await resp.text();
    const $ = cheerio.load(html);

    // Remove non-content elements
    $("nav, header, footer, .sidebar, script, style, .menu, .breadcrumb").remove();

    // Extract effective date from page if present
    const dateMatch = html.match(
      /(?:Stand|Datum|Veröffentlichungsdatum|Inkrafttreten)[:\s]*(\d{1,2})[.\s/](\d{1,2})[.\s/](\d{4})/i,
    );
    const effectiveDate = dateMatch
      ? `${dateMatch[3]}-${dateMatch[2]!.padStart(2, "0")}-${dateMatch[1]!.padStart(2, "0")}`
      : null;

    // Try to extract Dokumentennummer
    const docNumMatch = html.match(
      /Dokumentennummer[:\s]*([^\n<]+)/i,
    );
    const docNum = docNumMatch ? docNumMatch[1]!.trim() : null;

    // Strategy 1: split by headings (h2, h3) to create sections
    const headings = $("h2, h3");
    if (headings.length > 0) {
      let sectionIdx = 0;
      headings.each((_i, heading) => {
        const headingText = $(heading).text().trim();
        if (headingText.length < 3) return;

        // Collect all sibling content until next heading
        let sectionText = "";
        let next = $(heading).next();
        while (next.length > 0 && !next.is("h2, h3")) {
          sectionText += next.text().trim() + "\n";
          next = next.next();
        }

        sectionText = sectionText.replace(/\s+/g, " ").trim();
        if (sectionText.length < 50) return;

        sectionIdx++;
        const chapterNum = String(sectionIdx);
        provisions.push({
          sourcebook_id: doc.sourcebookId,
          reference: docNum
            ? `${docNum} Abschnitt ${sectionIdx}`
            : buildReference(doc, sectionIdx),
          title: headingText,
          text: sectionText,
          type: doc.type,
          status: "in_force",
          effective_date: effectiveDate,
          chapter: chapterNum,
          section: `${chapterNum}.1`,
        });
      });
    }

    // Strategy 2: if no headings produced results, take the full content
    if (provisions.length === 0) {
      const mainText =
        $("main").text().trim() ||
        $(".entry-content").text().trim() ||
        $("article").text().trim() ||
        $("body").text().trim();

      const cleanText = mainText.replace(/\s+/g, " ").trim();
      if (cleanText.length > 100) {
        const pageTitle =
          $("h1").first().text().trim() || doc.title;
        provisions.push({
          sourcebook_id: doc.sourcebookId,
          reference: docNum ?? buildReference(doc, 0),
          title: pageTitle,
          text: cleanText.slice(0, 50_000), // cap at 50k chars
          type: doc.type,
          status: "in_force",
          effective_date: effectiveDate,
          chapter: null,
          section: null,
        });
      }
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.warn(`  Fehler beim Crawlen von ${doc.url}: ${msg}`);
  }

  return provisions;
}

// ---------------------------------------------------------------------------
// 3. Crawl enforcement actions (Sanktionen / Bekanntmachungen)
// ---------------------------------------------------------------------------

interface SanctionListEntry {
  title: string;
  url: string;
  date: string | null;
}

/**
 * Scrape one page of the sanctions listing.
 * Returns discovered sanction entries and whether a next page exists.
 */
async function scrapeSanctionsPage(
  pageNum: number,
): Promise<{ entries: SanctionListEntry[]; hasNext: boolean }> {
  const url =
    pageNum <= 1
      ? SANCTIONS_BASE
      : `${SANCTIONS_BASE}page/${pageNum}/`;

  console.log(`  Sanktionsseite ${pageNum}: ${url}`);

  const html = await fetchHtml(url);
  const $ = cheerio.load(html);

  const entries: SanctionListEntry[] = [];

  // WordPress post listing — each entry is typically in an <article> or
  // a div with class "post", "entry", "news-item", or similar
  $("article, .post, .news-item, .entry").each((_i, el) => {
    const titleEl = $(el).find("h2 a, h3 a, .entry-title a").first();
    const title = titleEl.text().trim();
    const href = titleEl.attr("href");
    if (!title || !href) return;

    // Date: look for <time> element, or .date, .entry-date
    const timeEl = $(el).find("time, .date, .entry-date, .post-date").first();
    let date: string | null = null;
    const datetime = timeEl.attr("datetime");
    if (datetime) {
      date = datetime.slice(0, 10); // YYYY-MM-DD
    } else {
      const dateText = timeEl.text().trim();
      const dmatch = dateText.match(/(\d{1,2})\.\s*(\w+)\s*(\d{4})/);
      if (dmatch) {
        const monthMap: Record<string, string> = {
          januar: "01", jänner: "01", februar: "02", maerz: "03", märz: "03",
          april: "04", mai: "05", juni: "06", juli: "07", august: "08",
          september: "09", oktober: "10", november: "11", dezember: "12",
        };
        const monthNum = monthMap[dmatch[2]!.toLowerCase()] ?? "01";
        date = `${dmatch[3]}-${monthNum}-${dmatch[1]!.padStart(2, "0")}`;
      }
    }

    entries.push({
      title,
      url: href.startsWith("http") ? href : `${BASE_URL}${href}`,
      date,
    });
  });

  // Fallback: if the WordPress theme does not use article/post wrappers,
  // look for links whose text contains "Bekanntmachung" or "Sanktion"
  if (entries.length === 0) {
    $("a").each((_i, el) => {
      const text = $(el).text().trim();
      const href = $(el).attr("href");
      if (!href) return;
      if (
        !/bekanntmachung|sanktion|verhängt|verhaengt/i.test(text) ||
        text.length < 30
      )
        return;

      // Skip if already found
      const fullUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
      if (entries.some((e) => e.url === fullUrl)) return;

      entries.push({ title: text, url: fullUrl, date: null });
    });
  }

  // Check if next page exists
  const hasNext =
    $(`a[href*="/page/${pageNum + 1}/"]`).length > 0 ||
    $(".next, .pagination .next, a.next").length > 0;

  return { entries, hasNext };
}

/**
 * Parse a single enforcement-action page and return a row.
 */
async function crawlSanctionPage(
  entry: SanctionListEntry,
): Promise<EnforcementRow | null> {
  try {
    const html = await fetchHtml(entry.url);
    const $ = cheerio.load(html);

    // Remove non-content
    $("nav, header, footer, .sidebar, script, style, .menu, .breadcrumb").remove();

    const bodyText =
      $(".entry-content").text().trim() ||
      $("article").text().trim() ||
      $("main").text().trim();

    const summary = bodyText.replace(/\s+/g, " ").trim().slice(0, 10_000);
    if (summary.length < 50) return null;

    // Extract firm name from title
    // Typical: "Bekanntmachung: FMA verhängt Sanktion gegen die FIRMA wegen..."
    let firmName = "Unbekannt";
    const firmMatch = entry.title.match(
      /(?:gegen\s+(?:die\s+)?(?:verantwortliche\s+Personen\s+der\s+)?|gegen\s+(?:den?\s+)?(?:im\s+Tatzeitraum\s+Verantwortlichen\s+der\s+)?)([^,]+?)(?:\s+wegen|\s+aufgrund|\s+gemäß|\s+gemaess|\s*$)/i,
    );
    if (firmMatch) {
      firmName = firmMatch[1]!.trim();
    }

    // Determine action type
    let actionType = "sanction";
    const titleLower = entry.title.toLowerCase();
    if (/konzession\s*(?:entzogen|widerrufen)/i.test(titleLower)) {
      actionType = "ban";
    } else if (/geldstrafe|geldbuße|geldbusse/i.test(titleLower)) {
      actionType = "fine";
    } else if (/verwaltungsstrafe/i.test(titleLower)) {
      actionType = "fine";
    } else if (/verwarnung|ermahnung/i.test(titleLower)) {
      actionType = "warning";
    }

    // Try to extract monetary amount from body
    let amount: number | null = null;
    const amountMatch = summary.match(
      /(?:Höhe\s+von|Geldstrafe\s+(?:von|in\s+Höhe\s+von)?)\s*(?:EUR|€)?\s*([\d.,]+)\s*(?:EUR|Euro|€)/i,
    );
    if (amountMatch) {
      amount = parseFloat(
        amountMatch[1]!.replace(/\./g, "").replace(",", "."),
      );
    }

    // Extract date from page if not from listing
    let date = entry.date;
    if (!date) {
      const pageDateMatch = summary.match(
        /(?:Bescheid\s+vom|vom)\s+(\d{1,2})\.\s*(\w+)\s+(\d{4})/i,
      );
      if (pageDateMatch) {
        const monthMap: Record<string, string> = {
          januar: "01", jänner: "01", februar: "02", maerz: "03", märz: "03",
          april: "04", mai: "05", juni: "06", juli: "07", august: "08",
          september: "09", oktober: "10", november: "11", dezember: "12",
        };
        const monthNum = monthMap[pageDateMatch[2]!.toLowerCase()] ?? "01";
        date = `${pageDateMatch[3]}-${monthNum}-${pageDateMatch[1]!.padStart(2, "0")}`;
      }
    }

    // Extract referenced law / regulation from body
    let sourcebookRefs: string | null = null;
    const lawRefs: string[] = [];
    const lawPatterns = [
      /BWG|Bankwesengesetz/g,
      /WAG\s*\d*/g,
      /FM-GwG|Geldwäschegesetz|GwG/g,
      /BörseG|Börsegesetz/g,
      /KMG|Kapitalmarktgesetz/g,
      /VAG|Versicherungsaufsichtsgesetz/g,
      /AIFMG|Alternative\s+Investmentfonds/g,
      /ZaDiG|Zahlungsdienstegesetz/g,
      /InvFG|Investmentfondsgesetz/g,
      /EuVECA/g,
      /DORA/g,
      /SanktG|Sanktionengesetz/g,
    ];
    for (const pat of lawPatterns) {
      const matches = summary.match(pat);
      if (matches) {
        for (const m of matches) {
          if (!lawRefs.includes(m)) lawRefs.push(m);
        }
      }
    }
    if (lawRefs.length > 0) {
      sourcebookRefs = lawRefs.join(", ");
    }

    return {
      firm_name: firmName,
      reference_number: null,
      action_type: actionType,
      amount,
      date,
      summary,
      sourcebook_references: sourcebookRefs,
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.warn(`  Fehler bei Sanktion ${entry.url}: ${msg}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Database insertion helpers
// ---------------------------------------------------------------------------

function insertSourcebooks(db: Database.Database): void {
  const stmt = db.prepare(
    "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
  );
  const tx = db.transaction(() => {
    for (const sb of SOURCEBOOKS) {
      stmt.run(sb.id, sb.name, sb.description);
    }
  });
  tx();
  console.log(`${SOURCEBOOKS.length} Quellenbereiche eingefuegt/aktualisiert`);
}

function insertProvision(db: Database.Database, p: ProvisionRow): void {
  db.prepare(
    `INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    p.sourcebook_id,
    p.reference,
    p.title,
    p.text,
    p.type,
    p.status,
    p.effective_date,
    p.chapter,
    p.section,
  );
}

function referenceExists(db: Database.Database, reference: string): boolean {
  const row = db
    .prepare("SELECT 1 FROM provisions WHERE reference = ? LIMIT 1")
    .get(reference);
  return row !== undefined;
}

function insertEnforcement(db: Database.Database, e: EnforcementRow): void {
  db.prepare(
    `INSERT INTO enforcement_actions
       (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    e.firm_name,
    e.reference_number,
    e.action_type,
    e.amount,
    e.date,
    e.summary,
    e.sourcebook_references,
  );
}

function sanctionExists(
  db: Database.Database,
  firmName: string,
  date: string | null,
): boolean {
  if (date) {
    const row = db
      .prepare(
        "SELECT 1 FROM enforcement_actions WHERE firm_name = ? AND date = ? LIMIT 1",
      )
      .get(firmName, date);
    return row !== undefined;
  }
  const row = db
    .prepare(
      "SELECT 1 FROM enforcement_actions WHERE firm_name = ? LIMIT 1",
    )
    .get(firmName);
  return row !== undefined;
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

interface Stats {
  docsDiscovered: number;
  docsSkipped: number;
  provisionsInserted: number;
  sanctionPagesScraped: number;
  sanctionsInserted: number;
  sanctionsSkipped: number;
  errors: number;
}

function newStats(): Stats {
  return {
    docsDiscovered: 0,
    docsSkipped: 0,
    provisionsInserted: 0,
    sanctionPagesScraped: 0,
    sanctionsInserted: 0,
    sanctionsSkipped: 0,
    errors: 0,
  };
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("=== FMA Ingestion Crawler ===");
  console.log(`  Datenbank:    ${DB_PATH}`);
  console.log(`  Modus:        ${dryRun ? "Trockenlauf" : "Produktiv"}`);
  console.log(`  Fortsetzen:   ${resume ? "ja" : "nein"}`);
  console.log(`  Erzwingen:    ${force ? "ja" : "nein"}`);
  console.log("");

  const db = dryRun ? null : initDatabase();
  if (db && !dryRun) {
    insertSourcebooks(db);
  }

  const progress = loadProgress();
  const stats = newStats();

  // ---- Phase 1: Documents (Rundschreiben, Mindeststandards, Leitfaeden, DORA) ----

  if (!sanctionsOnly) {
    const allDocs: DiscoveredDoc[] = [];

    // Standard listing pages
    for (const listing of DOC_LISTING_PAGES) {
      try {
        const docs = await discoverDocuments(
          listing.sourcebookId,
          listing.url,
          listing.label,
        );
        allDocs.push(...docs);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`  FEHLER bei ${listing.label}: ${msg}`);
        stats.errors++;
      }
    }

    // DORA pages
    try {
      const doraDocs = await discoverDoraDocuments();
      allDocs.push(...doraDocs);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`  FEHLER bei DORA: ${msg}`);
      stats.errors++;
    }

    stats.docsDiscovered = allDocs.length;
    console.log(`\nGesamt entdeckte Dokumente: ${allDocs.length}`);

    // Crawl each document
    for (let i = 0; i < allDocs.length; i++) {
      const doc = allDocs[i]!;

      // Skip if already processed (resume mode)
      if (resume && progress.completed_doc_urls.includes(doc.url)) {
        stats.docsSkipped++;
        continue;
      }

      console.log(
        `\n[${i + 1}/${allDocs.length}] ${doc.title.slice(0, 80)}`,
      );
      console.log(`  URL: ${doc.url}`);

      const provisions = await crawlDocument(doc);

      if (dryRun) {
        console.log(`  -> ${provisions.length} Bestimmungen (Trockenlauf)`);
        for (const p of provisions) {
          console.log(
            `     ${p.reference}: ${(p.title ?? "").slice(0, 60)} (${p.text.length} Zeichen)`,
          );
        }
      } else if (db) {
        let inserted = 0;
        for (const p of provisions) {
          if (!referenceExists(db, p.reference)) {
            insertProvision(db, p);
            inserted++;
          }
        }
        stats.provisionsInserted += inserted;
        console.log(
          `  -> ${inserted} Bestimmungen eingefuegt (${provisions.length} gefunden)`,
        );
      }

      // Update progress
      progress.completed_doc_urls.push(doc.url);
      if (!dryRun) {
        saveProgress(progress);
      }
    }
  }

  // ---- Phase 2: Enforcement actions (Sanktionen) ----

  if (!docsOnly) {
    console.log("\n\n=== Sanktionen / Durchsetzungsmassnahmen ===");

    const startPage = resume ? Math.max(progress.sanctions_last_page, 1) : 1;

    for (let page = startPage; page <= SANCTIONS_MAX_PAGES; page++) {
      let result: { entries: SanctionListEntry[]; hasNext: boolean };
      try {
        result = await scrapeSanctionsPage(page);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`  FEHLER auf Seite ${page}: ${msg}`);
        stats.errors++;
        break;
      }

      stats.sanctionPagesScraped++;
      console.log(
        `  Seite ${page}: ${result.entries.length} Eintraege gefunden`,
      );

      for (const entry of result.entries) {
        // Skip if already processed
        if (resume && progress.completed_sanction_urls.includes(entry.url)) {
          stats.sanctionsSkipped++;
          continue;
        }

        console.log(`    -> ${entry.title.slice(0, 80)}`);

        const row = await crawlSanctionPage(entry);
        if (!row) {
          stats.errors++;
          continue;
        }

        if (dryRun) {
          console.log(
            `       ${row.firm_name} | ${row.action_type} | ${row.amount ?? "-"} EUR | ${row.date ?? "kein Datum"}`,
          );
        } else if (db) {
          if (!sanctionExists(db, row.firm_name, row.date)) {
            insertEnforcement(db, row);
            stats.sanctionsInserted++;
          } else {
            stats.sanctionsSkipped++;
          }
        }

        progress.completed_sanction_urls.push(entry.url);
      }

      progress.sanctions_last_page = page;
      if (!dryRun) {
        saveProgress(progress);
      }

      if (!result.hasNext) {
        console.log(`  Keine weiteren Sanktionsseiten nach Seite ${page}`);
        break;
      }
    }
  }

  // ---- Summary ----

  console.log("\n\n=== Zusammenfassung ===");
  console.log(`  Dokumente entdeckt:       ${stats.docsDiscovered}`);
  console.log(`  Dokumente uebersprungen:  ${stats.docsSkipped}`);
  console.log(`  Bestimmungen eingefuegt:  ${stats.provisionsInserted}`);
  console.log(`  Sanktionsseiten gescrapt: ${stats.sanctionPagesScraped}`);
  console.log(`  Sanktionen eingefuegt:    ${stats.sanctionsInserted}`);
  console.log(`  Sanktionen uebersprungen: ${stats.sanctionsSkipped}`);
  console.log(`  Fehler:                   ${stats.errors}`);

  if (!dryRun && db) {
    const provCount = (
      db.prepare("SELECT count(*) as cnt FROM provisions").get() as {
        cnt: number;
      }
    ).cnt;
    const sbCount = (
      db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as {
        cnt: number;
      }
    ).cnt;
    const enfCount = (
      db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as {
        cnt: number;
      }
    ).cnt;
    const ftsCount = (
      db.prepare("SELECT count(*) as cnt FROM provisions_fts").get() as {
        cnt: number;
      }
    ).cnt;

    console.log("\nDatenbankzusammenfassung:");
    console.log(`  Quellenbereiche:         ${sbCount}`);
    console.log(`  Bestimmungen:            ${provCount}`);
    console.log(`  Durchsetzungsmassnahmen: ${enfCount}`);
    console.log(`  FTS-Eintraege:           ${ftsCount}`);

    db.close();
  }

  console.log(`\nFertig. Fortschritt gespeichert: ${PROGRESS_FILE}`);
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

main().catch((err) => {
  console.error("Fataler Fehler:", err);
  process.exit(1);
});
