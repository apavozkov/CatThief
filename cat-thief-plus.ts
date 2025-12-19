import JSZip from "jszip";
import { Agent, fetch } from "undici";

const CAT_URL = "http://algisothal.ru:8889/cat";
const CONCURRENCY = 3000;
const BATCH_SIZE = 12;
const MAX_CONNECTIONS = 4000;

const agent = new Agent({
  connections: MAX_CONNECTIONS,
  connectTimeout: 30_000,
  headersTimeout: 60_000,
  bodyTimeout: 60_000,
  keepAliveTimeout: 60_000,
});

const globalHashes = new Set<string>();
let currentBatch: Buffer[] = [];

let totalRequests = 0;
let uniqueImages = 0;
let BuiltArchives = 0;
let SentArchives = 0;

function logStartup() {
  console.log("========================================");
  console.log("Cat Thief Plus");
  console.log("========================================");
  console.log(`Целевой URL: ${CAT_URL}`);
  console.log(`Воркеры: ${CONCURRENCY}`);
  console.log(`Максимум соединений: ${MAX_CONNECTIONS}`);
  console.log(`Картинок в 1 архиве: ${BATCH_SIZE}`);
  console.log("========================================");
}

function logProgress() {
  console.log(
    `Уникальных изображений найдено: ${uniqueImages} | ` +
    `Всего сделано запросов: ${totalRequests}`
  );
}

function logShutdown() {
  console.log("\n========================================");
  console.log("Cat Thief Plus остановлен");
  console.log("========================================");
  console.log(`Всего отправлено запросов: ${totalRequests}`);
  console.log(`Найдено уникальных изображений: ${uniqueImages}`);
  console.log(`Всего создано архивов: ${BuiltArchives}`);
  console.log(`Всего отправлено архивов: ${SentArchives}`);
  console.log("========================================");
}

function logArchives() {
  console.log(`Создано архивов: ${BuiltArchives}`);
  console.log(`Отправлено архивов: ${SentArchives}`);
}

async function fetchCat(): Promise<Buffer> {
  totalRequests++;

  const res = await fetch(CAT_URL, {
    dispatcher: agent,
  });

  if (!res.ok) throw new Error("GET failed");

  return Buffer.from(await res.arrayBuffer());
}

function hashImage(buf: Buffer): string {
  return `${buf.length}:${buf.subarray(0, 64).toString("hex")}`;
}

async function buildZip(images: Buffer[]): Promise<Buffer> {
  const zip = new JSZip();

  images.forEach((img, i) => {
    zip.file(`cat_${i}.jpg`, img);
  });
  BuiltArchives++;
  return zip.generateAsync({ type: "nodebuffer" });
}

async function sendArchive(images: Buffer[]): Promise<void> {
  const zip = await buildZip(images);

  SentArchives++;

  fetch(CAT_URL, {
    method: "POST",
    dispatcher: agent,
    headers: {
      "Content-Type": "application/zip",
    },
    body: zip,
  }).catch(() => {

  });
}

async function worker(): Promise<void> {
  while (true) {
    try {
      const img = await fetchCat();
      const hash = hashImage(img);

      if (globalHashes.has(hash)) continue;

      globalHashes.add(hash);
      uniqueImages++;
      logProgress();

      currentBatch.push(img);

      if (currentBatch.length === BATCH_SIZE) {
        const batch = currentBatch;
        currentBatch = [];
        sendArchive(batch);
        logArchives();
      }
    } catch {

    }
  }
}

async function main() {
  logStartup();

  process.on("SIGINT", () => {
    logShutdown();
    process.exit(0);
  });

  process.on("SIGTERM", () => {
    logShutdown();
    process.exit(0);
  });

  await Promise.all(
    Array.from({ length: CONCURRENCY }, worker)
  );
}

main();
