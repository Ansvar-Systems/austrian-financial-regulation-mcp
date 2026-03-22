/**
 * Seed the FMA database with sample provisions for testing.
 *
 * Inserts representative provisions from FMA Rundschreiben, Mindeststandards,
 * and Leitfaeden so MCP tools can be tested without running a full ingestion.
 * Content is in German, as issued by the FMA.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["FMA_DB_PATH"] ?? "data/fma.db";
const force = process.argv.includes("--force");

// Bootstrap database

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log("Bestehende Datenbank geloescht: " + DB_PATH);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log("Datenbank initialisiert: " + DB_PATH);

// Sourcebooks

interface SourcebookRow {
  id: string;
  name: string;
  description: string;
}

const sourcebooks: SourcebookRow[] = [
  {
    id: "FMA_RUNDSCHREIBEN",
    name: "FMA Rundschreiben",
    description:
      "Aufsichtsrechtliche Rundschreiben der FMA zu IT-Sicherheit, Auslagerungen, Geldwaesche und weiteren regulatorischen Anforderungen.",
  },
  {
    id: "FMA_MINDESTSTANDARDS",
    name: "FMA Mindeststandards",
    description:
      "Verbindliche Mindeststandards der FMA fuer das Risikomanagement konzessionierter Unternehmen.",
  },
  {
    id: "FMA_LEITFAEDEN",
    name: "FMA Leitfaeden",
    description:
      "Nicht verbindliche Leitfaeden der FMA zu Governance, Compliance, internem Kontrollsystem und weiteren Themen.",
  },
  {
    id: "OENB_LEITFAEDEN",
    name: "OeNB Leitfaeden",
    description:
      "Leitfaeden der Oesterreichischen Nationalbank zu IT-Risiko, Zahlungsverkehr und Finanzstabilitaet.",
  },
];

const insertSourcebook = db.prepare(
  "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
);

for (const sb of sourcebooks) {
  insertSourcebook.run(sb.id, sb.name, sb.description);
}

console.log(sourcebooks.length + " Quellenbereiche eingefuegt");

// Sample provisions

interface ProvisionRow {
  sourcebook_id: string;
  reference: string;
  title: string;
  text: string;
  type: string;
  status: string;
  effective_date: string;
  chapter: string;
  section: string;
}

const provisions: ProvisionRow[] = [
  // FMA Rundschreiben zur IT-Sicherheit
  {
    sourcebook_id: "FMA_RUNDSCHREIBEN",
    reference: "RS-IT-2020 Abschnitt 1",
    title: "Anwendungsbereich IT-Sicherheitsrundschreiben",
    text: "Das vorliegende Rundschreiben richtet sich an alle von der FMA beaufsichtigten Kreditinstitute, Versicherungsunternehmen, Wertpapierfirmen und sonstigen Finanzdienstleistungsunternehmen. Es legt die Mindestanforderungen an die Informations- und Kommunikationstechnologie (IKT) sowie die IT-Sicherheit fest, die von den beaufsichtigten Unternehmen einzuhalten sind.",
    type: "Rundschreiben",
    status: "in_force",
    effective_date: "2020-06-01",
    chapter: "1",
    section: "1.1",
  },
  {
    sourcebook_id: "FMA_RUNDSCHREIBEN",
    reference: "RS-IT-2020 Abschnitt 3",
    title: "IT-Governance und Verantwortlichkeiten",
    text: "Die Geschaeftsleitung traegt die Gesamtverantwortung fuer eine angemessene IT-Governance. Sie hat sicherzustellen, dass eine klare Aufbau- und Ablauforganisation fuer den IKT-Bereich besteht, die Ressourcen fuer die IT-Sicherheit ausreichend bemessen sind, IKT-Risiken regelmaessig identifiziert, bewertet und gesteuert werden sowie ein umfassendes IKT-Notfallmanagement implementiert ist.",
    type: "Rundschreiben",
    status: "in_force",
    effective_date: "2020-06-01",
    chapter: "3",
    section: "3.1",
  },
  {
    sourcebook_id: "FMA_RUNDSCHREIBEN",
    reference: "RS-IT-2020 Abschnitt 5",
    title: "Informationssicherheitsmanagement",
    text: "Beaufsichtigte Unternehmen haben ein Informationssicherheitsmanagementsystem (ISMS) einzurichten, das den anerkannten internationalen Standards entspricht. Das ISMS hat eine Informationssicherheitspolitik, ein Klassifizierungsschema fuer Informationen, ein Risikomanagement fuer Informationssicherheit sowie regelmaessige Sicherheitsaudits und Penetrationstests zu umfassen.",
    type: "Rundschreiben",
    status: "in_force",
    effective_date: "2020-06-01",
    chapter: "5",
    section: "5.1",
  },
  {
    sourcebook_id: "FMA_RUNDSCHREIBEN",
    reference: "RS-AUS-2019 Abschnitt 2",
    title: "Anforderungen an Auslagerungen",
    text: "Bei der Auslagerung wesentlicher Funktionen und Taetigkeiten haben beaufsichtigte Unternehmen sicherzustellen, dass die Verantwortung der Geschaeftsleitung nicht beeintraechtigt wird, die Beziehungen und Pflichten des Unternehmens gegenueber seinen Kunden nicht wesentlich veraendert werden, die Voraussetzungen fuer die Konzession unveraendert erfuellt bleiben und die FMA die Aufsicht nicht beeintraechtigt wird.",
    type: "Rundschreiben",
    status: "in_force",
    effective_date: "2019-03-01",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "FMA_RUNDSCHREIBEN",
    reference: "RS-GW-2017 Abschnitt 1",
    title: "Sorgfaltspflichten zur Verhinderung von Geldwaesche",
    text: "Beaufsichtigte Unternehmen haben angemessene Massnahmen zur Verhinderung von Geldwaesche und Terrorismusfinanzierung zu ergreifen. Dazu gehoeren die Identifizierung und Verifizierung der Identitaet der Kunden und wirtschaftlichen Eigentuemer, die Beurteilung des Zwecks und der angestrebten Art der Geschaeftsbeziehung sowie eine laufende Ueberwachung der Geschaeftsbeziehung.",
    type: "Rundschreiben",
    status: "in_force",
    effective_date: "2017-01-01",
    chapter: "1",
    section: "1.2",
  },

  // FMA Mindeststandards fuer das Risikomanagement
  {
    sourcebook_id: "FMA_MINDESTSTANDARDS",
    reference: "MS-RISK-2018 Punkt 1",
    title: "Gesamtbankrisikostrategie",
    text: "Kreditinstitute haben eine Gesamtbankrisikostrategie zu entwickeln und zu implementieren, die mit der Unternehmensstrategie konsistent ist. Die Risikostrategie hat die wesentlichen Risikoarten, die das Institut eingehen will, zu beschreiben und fuer jede Risikoart die Risikobereitschaft und -tragfaehigkeit festzulegen.",
    type: "Mindeststandard",
    status: "in_force",
    effective_date: "2018-09-01",
    chapter: "1",
    section: "1.1",
  },
  {
    sourcebook_id: "FMA_MINDESTSTANDARDS",
    reference: "MS-RISK-2018 Punkt 3",
    title: "Internes Kontrollsystem",
    text: "Das interne Kontrollsystem (IKS) hat alle relevanten Risiken abzudecken und aus einer angemessenen Risikoidentifikation, -bewertung, -steuerung und -ueberwachung sowie aus Kontrollaktivitaeten und einem Berichtswesen zu bestehen. Die Interne Revision hat die Wirksamkeit und Angemessenheit des IKS regelmaessig zu pruefen.",
    type: "Mindeststandard",
    status: "in_force",
    effective_date: "2018-09-01",
    chapter: "3",
    section: "3.1",
  },
  {
    sourcebook_id: "FMA_MINDESTSTANDARDS",
    reference: "MS-RISK-2018 Punkt 5",
    title: "Stresstests und Szenarioanalysen",
    text: "Kreditinstitute haben regelmaessig Stresstests und Szenarioanalysen durchzufuehren, um ihre Anfaelligkeit gegenueber aussergewoehnlichen, aber plausiblen Ereignissen zu beurteilen. Die Ergebnisse der Stresstests sind in die Kapitalplanung und das Risikomanagement einzubeziehen.",
    type: "Mindeststandard",
    status: "in_force",
    effective_date: "2018-09-01",
    chapter: "5",
    section: "5.1",
  },

  // FMA Leitfaden Governance und Compliance
  {
    sourcebook_id: "FMA_LEITFAEDEN",
    reference: "LF-GOV-2021 Kapitel 2",
    title: "Aufgaben des Aufsichtsrats",
    text: "Der Aufsichtsrat hat die Geschaeftsfuehrung zu ueberwachen und dafuer Sorge zu tragen, dass ein effektives Risikomanagementsystem eingerichtet ist. Er hat sich regelmaessig ueber die Risikolage und die Risikotragfaehigkeit des Unternehmens informieren zu lassen und bei wesentlichen Aenderungen der Risikostrategie eingebunden zu sein.",
    type: "Leitfaden",
    status: "in_force",
    effective_date: "2021-01-01",
    chapter: "2",
    section: "2.1",
  },
  {
    sourcebook_id: "FMA_LEITFAEDEN",
    reference: "LF-GOV-2021 Kapitel 4",
    title: "Compliance-Funktion",
    text: "Die Compliance-Funktion hat sicherzustellen, dass das Unternehmen die geltenden gesetzlichen und aufsichtsrechtlichen Anforderungen einhalt. Sie hat Compliance-Risiken zu identifizieren, zu bewerten und zu ueberwachen sowie die Geschaeftsleitung und die Mitarbeiter in Compliance-Fragen zu beraten und zu schulen.",
    type: "Leitfaden",
    status: "in_force",
    effective_date: "2021-01-01",
    chapter: "4",
    section: "4.1",
  },

  // OeNB Leitfaeden
  {
    sourcebook_id: "OENB_LEITFAEDEN",
    reference: "OENB-IT-2013 Abschnitt 3",
    title: "IT-Risikomanagement: Governance und Strategie",
    text: "Ein wirksames IT-Risikomanagement setzt voraus, dass die Geschaeftsleitung die Verantwortung fuer IT-Risiken wahrnimmt, eine IT-Risikostrategie festlegt und genehmigt sowie angemessene Ressourcen und Kompetenzen fuer das IT-Risikomanagement bereitstellt. IT-Risiken sind in den gesamten Risikosteuerungsprozess der Bank zu integrieren.",
    type: "Leitfaden",
    status: "in_force",
    effective_date: "2013-10-01",
    chapter: "3",
    section: "3.1",
  },
];

const insertProvision = db.prepare(
  "INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
);

const insertAll = db.transaction(() => {
  for (const p of provisions) {
    insertProvision.run(
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
});

insertAll();

console.log(provisions.length + " Beispielbestimmungen eingefuegt");

// Sample enforcement actions

interface EnforcementRow {
  firm_name: string;
  reference_number: string;
  action_type: string;
  amount: number;
  date: string;
  summary: string;
  sourcebook_references: string;
}

const enforcements: EnforcementRow[] = [
  {
    firm_name: "Commerzialbank Mattersburg im Burgenland AG",
    reference_number: "FMA-GZ-2020-001",
    action_type: "ban",
    amount: 0,
    date: "2020-07-14",
    summary:
      "Die FMA hat mit Bescheid vom 14. Juli 2020 der Commerzialbank Mattersburg im Burgenland AG die Konzession zum Betrieb von Bankgeschaeften entzogen und die Abwicklung der Bank angeordnet. Grund war die Feststellung massiver Bilanzbetruege ueber mehrere Jahrzehnte, bei denen Bankguthaben und Wertpapiere in Hoehe von ueber 700 Millionen Euro vorgespiegelt wurden, die nicht existierten. Die FMA-Pruefung ergab, dass die internen Kontrollen vollstaendig versagt hatten.",
    sourcebook_references: "MS-RISK-2018 Punkt 3, LF-GOV-2021 Kapitel 2",
  },
  {
    firm_name: "Anglo Austrian AAB Bank AG (ehemals Meinl Bank AG)",
    reference_number: "FMA-GZ-2020-045",
    action_type: "ban",
    amount: 0,
    date: "2020-02-14",
    summary:
      "Die FMA hat der Anglo Austrian AAB Bank AG die Konzession fuer den Betrieb von Bankgeschaeften entzogen. Die Bank hatte trotz wiederholter Aufforderungen schwerwiegende Maengel in der Geldwaeschepraeventionen nicht behoben. Darueber hinaus wurden erhebliche Governance-Schwaechen und mangelhafte Dokumentation von Kundentransaktionen festgestellt.",
    sourcebook_references: "RS-GW-2017 Abschnitt 1, LF-GOV-2021 Kapitel 4",
  },
  {
    firm_name: "Dadat Bank AG",
    reference_number: "FMA-GZ-2022-112",
    action_type: "fine",
    amount: 120_000,
    date: "2022-09-01",
    summary:
      "Die FMA hat gegen die Dadat Bank AG eine Geldstrafe in Hoehe von 120.000 Euro wegen Verstoessen gegen die Wohlverhaltensregeln im Wertpapierbereich verhaengt. Das Unternehmen hatte Kunden nicht ausreichend ueber Risiken aufgeklaert und die Eignung von Anlageempfehlungen nicht in allen Faellen ordnungsgemaess dokumentiert.",
    sourcebook_references: "LF-GOV-2021 Kapitel 4",
  },
];

const insertEnforcement = db.prepare(
  "INSERT INTO enforcement_actions (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references) VALUES (?, ?, ?, ?, ?, ?, ?)",
);

const insertEnforcementsAll = db.transaction(() => {
  for (const e of enforcements) {
    insertEnforcement.run(
      e.firm_name,
      e.reference_number,
      e.action_type,
      e.amount,
      e.date,
      e.summary,
      e.sourcebook_references,
    );
  }
});

insertEnforcementsAll();

console.log(enforcements.length + " Durchsetzungsmassnahmen eingefuegt");

// Summary

const provisionCount = (
  db.prepare("SELECT count(*) as cnt FROM provisions").get() as { cnt: number }
).cnt;
const sourcebookCount = (
  db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as { cnt: number }
).cnt;
const enforcementCount = (
  db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as { cnt: number }
).cnt;
const ftsCount = (
  db.prepare("SELECT count(*) as cnt FROM provisions_fts").get() as { cnt: number }
).cnt;

console.log("\nDatenbankzusammenfassung:");
console.log("  Quellenbereiche:         " + sourcebookCount);
console.log("  Bestimmungen:            " + provisionCount);
console.log("  Durchsetzungsmassnahmen: " + enforcementCount);
console.log("  FTS-Eintraege:           " + ftsCount);
console.log("\nFertig. Datenbank bereit: " + DB_PATH);

db.close();
