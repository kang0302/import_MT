// scripts/barometer_snapshot.mjs
// A. 포워드 스냅샷: 매일 전 테마 바로미터를 계산해 날짜별로 append (무편향 트랙레코드 누적).
// 사용법: node scripts/barometer_snapshot.mjs [YYYY-MM-DD]
//   날짜 미지정 시 오늘. 산출: import_MT/data/barometer/{date}.json + index.json
import fs from "fs";
import path from "path";
import { computeThemeBarometer, PERIODS } from "./barometer_core.mjs";

const ROOT = process.cwd();
// 루트 무관: moneytree-web 루트(import_MT/data/theme) 또는 import_MT 루트(data/theme) 자동 감지.
const DATA_ROOT = fs.existsSync(path.join(ROOT, "import_MT", "data", "theme"))
  ? path.join(ROOT, "import_MT", "data")
  : path.join(ROOT, "data");
const THEME_DIR = path.join(DATA_ROOT, "theme");
const OUT_DIR = path.join(DATA_ROOT, "barometer");

function today() {
  const d = new Date();
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}
const DATE = process.argv[2] || today();

fs.mkdirSync(OUT_DIR, { recursive: true });
const files = fs.readdirSync(THEME_DIR).filter((f) => /^T_\d+\.json$/.test(f));

const rows = [];
let okCount = 0;
for (const f of files) {
  let d;
  try {
    d = JSON.parse(fs.readFileSync(path.join(THEME_DIR, f), "utf8"));
  } catch {
    continue;
  }
  const themeId = d.themeId || f.replace(".json", "");
  const periodDefault = (d.meta?.periodDefault || "7d").toString();
  const scores = {};
  const avgRet = {};
  let ok = false;
  for (const p of PERIODS) {
    const r = computeThemeBarometer({ nodes: d.nodes, edges: d.edges, period: p });
    if (r.ok) {
      scores[p] = r.overallScore;
      avgRet[p] = Number(r.avgReturn.toFixed(3));
      ok = true;
    } else {
      scores[p] = null;
      avgRet[p] = null;
    }
  }
  const hp = (periodDefault || "7d").toUpperCase();
  const headlinePeriod = PERIODS.includes(hp) ? hp : "7D";
  const assetCount = (d.nodes || []).filter((n) => (n.type ?? "").toUpperCase() === "ASSET").length;
  if (ok) okCount++;
  rows.push({
    themeId,
    themeName: d.themeName || "",
    ok,
    assetCount,
    headlinePeriod,
    headlineScore: scores[headlinePeriod] ?? null,
    scores, // overallScore per period
    avgRet, // 가중 EW 수익률(%) per period — forward 매칭·검증용
  });
}

const payload = { date: DATE, generated: new Date().toISOString(), themeCount: rows.length, okCount, rows };
const outFile = path.join(OUT_DIR, `${DATE}.json`);
fs.writeFileSync(outFile, JSON.stringify(payload, null, 2), "utf8");

// index.json (날짜 목록, 최신순)
const idxFile = path.join(OUT_DIR, "index.json");
let idx = [];
try {
  idx = JSON.parse(fs.readFileSync(idxFile, "utf8"));
} catch {}
if (!Array.isArray(idx)) idx = [];
if (!idx.some((x) => x.date === DATE)) idx.push({ date: DATE, themeCount: rows.length, okCount });
idx.sort((a, b) => (a.date < b.date ? 1 : -1));
fs.writeFileSync(idxFile, JSON.stringify(idx, null, 2), "utf8");

console.log(`✅ barometer snapshot ${DATE}: ${rows.length} themes (${okCount} scored) → ${path.relative(ROOT, outFile)}`);
