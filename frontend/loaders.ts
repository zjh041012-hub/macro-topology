// data/loaders.ts —— 真实数据版
// 替换 Codex 项目中的同名文件即可。视觉层零改动。
//
// DATA_BASE 三种部署方式任选:
//   1. 同仓库 GitHub Pages:  "/macro-topology/output"
//   2. raw.githubusercontent: "https://raw.githubusercontent.com/<you>/<repo>/main/output"
//   3. 本地开发:              将 output/*.json 复制到 public/data 后用 "/data"

import type {
  MacroNode, MacroEdge, MacroPath, DivergenceRecord, MarketState,
} from "../types/topology";

const DATA_BASE =
  import.meta.env.VITE_DATA_BASE ?? "/data";

async function getJSON<T>(name: string): Promise<T> {
  const res = await fetch(`${DATA_BASE}/${name}?t=${Date.now()}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`load ${name} failed: ${res.status}`);
  return res.json() as Promise<T>;
}

export const loadTopologyNodes = () => getJSON<MacroNode[]>("nodes.json");
export const loadTopologyEdges = () => getJSON<MacroEdge[]>("edges.json");
export const loadTopologyPaths = () => getJSON<MacroPath[]>("paths.json");
export const loadDivergences = () => getJSON<DivergenceRecord[]>("divergences.json");
export const loadCurrentMarketState = () => getJSON<MarketState>("market_state.json");

// 失败兜底: 若任一文件加载失败, 调用方可回退到打包内的 mock 数据
export async function loadAllWithFallback(mock: {
  nodes: MacroNode[]; edges: MacroEdge[]; paths: MacroPath[];
  divergences: DivergenceRecord[]; marketState: MarketState;
}) {
  try {
    const [nodes, edges, paths, divergences, marketState] = await Promise.all([
      loadTopologyNodes(), loadTopologyEdges(), loadTopologyPaths(),
      loadDivergences(), loadCurrentMarketState(),
    ]);
    return { nodes, edges, paths, divergences, marketState, live: true };
  } catch (e) {
    console.warn("[loaders] live data unavailable, fallback to mock:", e);
    return { ...mock, live: false };
  }
}
