import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const apiGet = vi.fn();

vi.mock("../api", () => ({
  api: { GET: apiGet },
  cityScope: () => (new URLSearchParams(window.location.search).get("city") ?? "").trim(),
  mutationHeaders: {},
}));

function installIssuesDOM(): void {
  document.body.innerHTML = `
    <span id="issues-count">0</span>
    <div class="panel-tabs rig-filter-tabs" id="rig-filter-tabs">
      <button class="rig-btn active" data-rig="all">All</button>
    </div>
    <div id="issues-list"></div>
    <div id="issue-detail" style="display: none;"></div>
  `;
}

function ok(data: unknown): { data: unknown } {
  return { data };
}

describe("issues panel rig filter", () => {
  beforeEach(() => {
    vi.resetModules();
    apiGet.mockReset();
    installIssuesDOM();
    window.history.pushState({}, "", "/dashboard?city=test-city");
  });

  afterEach(() => {
    window.history.pushState({}, "", "/dashboard");
  });

  it("labels rig buttons by name, filters by prefix, and shows the rig name in the row", async () => {
    apiGet.mockImplementation((path: string, opts: { params: { query?: { status?: string } } }) => {
      if (path === "/v0/city/{cityName}/beads") {
        const status = opts.params.query?.status;
        if (status === "open") {
          return Promise.resolve(ok({
            items: [
              { id: "tk-001", title: "tool bead", priority: 2, status: "open" },
              { id: "sl-002", title: "signal bead", priority: 2, status: "open" },
            ],
          }));
        }
        return Promise.resolve(ok({ items: [] }));
      }
      throw new Error(`unexpected GET ${path}`);
    });

    const options = await import("./options");
    vi.spyOn(options, "getOptions").mockResolvedValue({
      agents: [],
      beads: [],
      fetchedAt: Date.now(),
      mail: [],
      rigs: [
        { name: "gc-toolkit", prefix: "tk" },
        { name: "signal-loom", prefix: "sl" },
      ],
      sessions: [],
    });

    const { renderIssues } = await import("./issues");
    await renderIssues();

    const rigBtns = Array.from(document.querySelectorAll<HTMLElement>(".rig-btn"));
    const labels = rigBtns.map((btn) => btn.textContent);
    const values = rigBtns.map((btn) => btn.dataset.rig);
    expect(labels).toEqual(["All", "gc-toolkit", "signal-loom"]);
    expect(values).toEqual(["all", "tk", "sl"]);

    // Before filtering: both beads render, Rig column shows the rig NAME, not the prefix.
    const rigCells = Array.from(document.querySelectorAll<HTMLElement>(".issue-rig"));
    expect(rigCells.map((td) => td.textContent)).toEqual(["gc-toolkit", "signal-loom"]);

    // Click the "gc-toolkit" button — should keep only the tk- bead.
    const toolkitBtn = rigBtns.find((btn) => btn.dataset.rig === "tk")!;
    toolkitBtn.click();

    const visibleIDs = Array.from(document.querySelectorAll<HTMLElement>(".issue-id"))
      .map((node) => node.textContent);
    expect(visibleIDs).toEqual(["tk-001"]);
  });

  it("falls back to the prefix when a bead's prefix isn't in the rig list", async () => {
    apiGet.mockImplementation((path: string, opts: { params: { query?: { status?: string } } }) => {
      if (path === "/v0/city/{cityName}/beads") {
        if (opts.params.query?.status === "open") {
          return Promise.resolve(ok({
            items: [{ id: "zz-099", title: "orphan", priority: 2, status: "open" }],
          }));
        }
        return Promise.resolve(ok({ items: [] }));
      }
      throw new Error(`unexpected GET ${path}`);
    });

    const options = await import("./options");
    vi.spyOn(options, "getOptions").mockResolvedValue({
      agents: [],
      beads: [],
      fetchedAt: Date.now(),
      mail: [],
      rigs: [{ name: "gc-toolkit", prefix: "tk" }],
      sessions: [],
    });

    const { renderIssues } = await import("./issues");
    await renderIssues();

    const rigCells = Array.from(document.querySelectorAll<HTMLElement>(".issue-rig"));
    expect(rigCells.map((td) => td.textContent)).toEqual(["zz"]);
  });
});
