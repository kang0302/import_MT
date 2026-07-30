// scripts/barometer_core.mjs
// src/lib/themeReturn.ts 의 바로미터 계산을 Node(.mjs)로 충실 이식.
// 프런트와 동일 결과를 내야 트랙레코드가 유효 — 로직 변경 금지(원본과 1:1).

export const PERIODS = ["1D", "3D", "7D", "15D", "1M", "YTD", "1Y", "2Y", "3Y"];

const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));
const wsum = (ws) => ws.reduce((a, b) => a + b, 0);
function wmean(vals, ws) {
  const sw = wsum(ws);
  if (sw <= 0) return 0;
  let s = 0;
  for (let i = 0; i < vals.length; i++) s += vals[i] * ws[i];
  return s / sw;
}
function wmedian(vals, ws) {
  if (!vals.length) return 0;
  const idx = vals.map((_, i) => i).sort((a, b) => vals[a] - vals[b]);
  const half = wsum(ws) / 2;
  let cum = 0;
  for (const i of idx) {
    cum += ws[i];
    if (cum >= half) return vals[i];
  }
  return vals[idx[idx.length - 1]];
}

export function normalizePeriodKey(p) {
  if (p === null || p === undefined) return null;
  const raw = String(p).trim();
  if (raw === "1" || raw.toLowerCase() === "1d" || raw === "1일") return "1D";
  if (raw === "3" || raw.toLowerCase() === "3d" || raw === "3일") return "3D";
  if (raw === "7" || raw.toLowerCase() === "7d" || raw === "7일") return "7D";
  if (raw === "15" || raw.toLowerCase() === "15d" || raw === "15일") return "15D";
  if (raw.toLowerCase() === "1m" || raw === "1개월" || raw === "1달") return "1M";
  if (raw.toLowerCase() === "ytd" || raw === "연초" || raw === "올해") return "YTD";
  if (raw.toLowerCase() === "1y" || raw === "1년") return "1Y";
  if (raw.toLowerCase() === "2y" || raw === "2년") return "2Y";
  if (raw.toLowerCase() === "3y" || raw === "3년") return "3Y";
  const up = raw.toUpperCase();
  if (["1D","3D","7D","15D","1M","YTD","1Y","2Y","3Y"].includes(up)) return up;
  return null;
}

function normalizeToPct(v) {
  if (v === null || v === undefined) return null;
  let n;
  if (typeof v === "number") n = v;
  else if (typeof v === "string") {
    const cleaned = v.trim().replace(/,/g, "");
    if (!cleaned) return null;
    n = Number(cleaned);
  } else return null;
  if (!Number.isFinite(n)) return null;
  return n;
}

export function extractReturnByPeriod(metrics, periodRaw) {
  if (!metrics) return null;
  const period = normalizePeriodKey(periodRaw);
  if (!period) return null;
  const live = metrics._liveReturn;
  const livePeriodRaw = metrics._liveReturnPeriod;
  if (typeof live === "number" && Number.isFinite(live)) {
    const livePeriod = livePeriodRaw == null ? null : normalizePeriodKey(livePeriodRaw);
    if (livePeriod == null || livePeriod === period) return live;
  }
  const pick = (...keys) => {
    for (const k of keys) {
      const v = normalizeToPct(metrics[k]);
      if (v !== null) return v;
    }
    return null;
  };
  switch (period) {
    case "1D": return pick("return_1d","return_1D","return1d","ret_1d","ret1d");
    case "3D": return pick("return_3d","return_3D","return3d","ret_3d","ret3d");
    case "7D": return pick("return_7d","return_7D","return7d","ret_7d","ret7d");
    case "15D": return pick("return_15d","return_15D","return15d","ret_15d","ret15d");
    case "1M": return pick("return_1m","return_30d","return_30D","return1m","return30d","ret_1m","ret_30d","ret1m","ret30d");
    case "YTD": return pick("return_ytd","return_YTD","returnYtd","ret_ytd","retYtd");
    case "1Y": return pick("return_1y","return_1Y","return1y","ret_1y","ret1y");
    case "2Y": return pick("return_2y","return_2Y","return2y","ret_2y","ret2y");
    case "3Y": return pick("return_3y","return_3Y","return3y","ret_3y","ret3y");
    default: return null;
  }
}

export function computeOrbitWeights(assetIds, nodes, edges) {
  const w = new Map();
  const nodeW = new Map();
  for (const n of nodes) {
    const nw = n.weight;
    if (typeof nw === "number" && Number.isFinite(nw)) nodeW.set(n.id, nw);
  }
  if (!edges || !edges.length) {
    for (const id of assetIds) w.set(id, nodeW.has(id) ? nodeW.get(id) : 1);
    return w;
  }
  const themeId = (nodes.find((n) => (n.type ?? "").toUpperCase() === "THEME") || {}).id;
  const direct = new Set();
  const edgeW = new Map();
  for (const e of edges) {
    if ((e.type ?? "").toUpperCase() === "THEMED_AS" && e.from && (!themeId || e.to === themeId)) direct.add(e.from);
    if (e.from && typeof e.weight === "number" && Number.isFinite(e.weight)) {
      edgeW.set(e.from, Math.min(edgeW.get(e.from) ?? Infinity, e.weight));
    }
  }
  const pick = (id, orbit) => (nodeW.has(id) ? nodeW.get(id) : edgeW.has(id) ? edgeW.get(id) : orbit);
  if (direct.size === 0) {
    for (const id of assetIds) w.set(id, pick(id, 1));
    return w;
  }
  for (const id of assetIds) w.set(id, pick(id, direct.has(id) ? 1 : 0.5));
  return w;
}

const PERIOD_ANCHORS = {
  "1D": { retSat: 4, tailThresh: 5 },
  "3D": { retSat: 6, tailThresh: 8 },
  "7D": { retSat: 9, tailThresh: 12 },
  "15D": { retSat: 13, tailThresh: 15 },
  "1M": { retSat: 16.7, tailThresh: 15 },
  YTD: { retSat: 30, tailThresh: 25 },
  "1Y": { retSat: 50, tailThresh: 40 },
  "2Y": { retSat: 75, tailThresh: 55 },
  "3Y": { retSat: 100, tailThresh: 70 },
};
const DEFAULT_ANCHOR = PERIOD_ANCHORS["1M"];
function anchorForPeriod(period) {
  const k = normalizePeriodKey(period);
  return (k && PERIOD_ANCHORS[k]) || DEFAULT_ANCHOR;
}
const scoreReturnPct = (retPct, retSat) => clamp(500 + retPct * (500 / retSat), 0, 1000);
const scoreBreadthPct = (b) => clamp(b * 10, 0, 1000);
function scoreDiversification(breadthPct, gapPct = 0, retSat = 16.7) {
  const base = clamp(breadthPct, 0, 100) * 10;
  const dispersion = clamp(retSat > 0 ? gapPct / retSat : 0, 0, 2) / 2;
  return clamp(base * (1 - 0.5 * dispersion), 0, 1000);
}
const scoreRiskFromTailPct = (t) => clamp(1000 - t * 10, 0, 1000);
function calcOverall({ healthScore, momentumScore, divScore, tailPct }) {
  const riskScore = scoreRiskFromTailPct(tailPct);
  const overallScore = clamp(healthScore * 0.35 + momentumScore * 0.35 + divScore * 0.2 + riskScore * 0.1, 0, 1000);
  return { overallScore: Math.round(overallScore), riskScore: Math.round(riskScore) };
}
export function tempByScore(score) {
  const s = clamp(score, 0, 1000);
  if (s >= 900) return "BLAZING";
  if (s >= 800) return "HOT";
  if (s >= 700) return "WARM+";
  if (s >= 600) return "WARM";
  if (s >= 500) return "NEUTRAL+";
  if (s >= 400) return "NEUTRAL";
  if (s >= 300) return "COOL";
  if (s >= 200) return "COOL-";
  if (s >= 100) return "COLD";
  return "FROZEN";
}
function computeGapPctW(returns, weights) {
  const n = returns.length;
  if (n < 2) return 0;
  const idx = returns.map((_, i) => i).sort((a, b) => returns[a] - returns[b]);
  const bucketW = wsum(weights) * 0.3;
  const pick = (order) => {
    const vs = [], ws = [];
    let cw = 0;
    for (const i of order) {
      vs.push(returns[i]); ws.push(weights[i]); cw += weights[i];
      if (cw >= bucketW) break;
    }
    return wmean(vs, ws);
  };
  return pick([...idx].reverse()) - pick(idx);
}

/**
 * 테마 노드/엣지 + 기간 → 바로미터 요약 (원본 computeThemeReturnSummary 이식).
 * metricsOverride(id→metrics)를 주면 그 값으로 계산(백테스트용). 없으면 node.metrics 사용.
 */
export function computeThemeBarometer({ nodes, edges, period, minAssets = 5, metricsOverride = null }) {
  const assets = (Array.isArray(nodes) ? nodes : []).filter((n) => (n.type ?? "").toUpperCase() === "ASSET");
  const assetCount = assets.length;
  const withRet = assets
    .map((a) => {
      const metrics = metricsOverride ? metricsOverride.get(a.id) : a.metrics;
      const ret = extractReturnByPeriod(metrics, period);
      return { id: a.id, name: a.name, ret };
    })
    .filter((x) => typeof x.ret === "number" && Number.isFinite(x.ret));
  const returns = withRet.map((x) => x.ret);
  const validN = returns.length;
  if (assetCount < minAssets) return { ok: false, reason: "MIN_ASSET_NOT_MET", assetCount, validReturnCount: validN };
  if (validN === 0) return { ok: false, reason: "NO_RETURN_DATA", assetCount, validReturnCount: 0 };

  const wmap = computeOrbitWeights(withRet.map((x) => x.id), nodes, edges);
  const weights = withRet.map((x) => wmap.get(x.id) ?? 1);
  const totalW = wsum(weights);
  const coreMedianPct = wmedian(returns, weights);
  const avgReturn = wmean(returns, weights);
  const orderDesc = returns.map((_, i) => i).sort((a, b) => returns[b] - returns[a]);
  const topN = validN >= 10 ? Math.ceil(validN * 0.3) : 2;
  const topIdx = orderDesc.slice(0, clamp(topN, 1, validN));
  const momentumTopPct = wmean(topIdx.map((i) => returns[i]), topIdx.map((i) => weights[i]));
  const breadthPct = (returns.reduce((acc, r, i) => acc + (r > 0 ? weights[i] : 0), 0) / totalW) * 100;
  const anchor = anchorForPeriod(period);
  const tailPct = (returns.reduce((acc, r, i) => acc + (Math.abs(r) >= anchor.tailThresh ? weights[i] : 0), 0) / totalW) * 100;
  const gapPct = computeGapPctW(returns, weights);
  const robustCenter = 0.5 * avgReturn + 0.5 * coreMedianPct;
  const levelScore = scoreReturnPct(robustCenter, anchor.retSat);
  const breadthScore = scoreBreadthPct(breadthPct);
  const healthScore = clamp(levelScore * 0.6 + breadthScore * 0.4, 0, 1000);
  const momentumScore = scoreReturnPct(momentumTopPct, anchor.retSat);
  const divScore = scoreDiversification(breadthPct, gapPct, anchor.retSat);
  const { overallScore, riskScore } = calcOverall({ healthScore, momentumScore, divScore, tailPct });
  return {
    ok: true, assetCount, validReturnCount: validN,
    overallScore, temp: tempByScore(overallScore),
    healthScore: Math.round(healthScore), momentumScore: Math.round(momentumScore),
    divScore: Math.round(divScore), riskScore,
    avgReturn, coreMedianPct, momentumTopPct, breadthPct, tailPct,
  };
}
