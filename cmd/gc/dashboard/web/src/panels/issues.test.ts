import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import indexHTML from "../../index.html?raw";
import type { BeadRecord } from "../api";
import { renderIssues } from "./issues";
import * as options from "./options";

const { getMock } = vi.hoisted(() => ({ getMock: vi.fn() }));

vi.mock("../api", () => ({
  api: {
    GET: getMock,
    POST: vi.fn(),
  },
  cityScope: vi.fn(() => "test-city"),
  mutationHeaders: { "X-GC-Request": "true" },
}));

vi.mock("../ui", () => ({
  popPause: vi.fn(),
  pushPause: vi.fn(),
  showToast: vi.fn(),
}));

async function waitFor(assertion: () => void | Promise<void>): Promise<void> {
  const deadline = Date.now() + 2_000;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      await assertion();
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }
  throw lastError;
}

function installDOM(): void {
  document.body.innerHTML = `
    <span id="issues-count"></span>
    <div id="rig-filter-tabs"></div>
    <div id="issues-list"></div>
    <div id="issue-detail" style="display: none;">
      <span id="issue-detail-priority" class="badge"></span>
      <span id="issue-detail-id" class="issue-id"></span>
      <span id="issue-detail-status" class="issue-status"></span>
      <h3 id="issue-detail-title-text"></h3>
      <div class="issue-detail-meta">
        <span id="issue-detail-type"></span>
        <span id="issue-detail-owner"></span>
        <span id="issue-detail-created"></span>
        <span id="issue-detail-updated"></span>
      </div>
      <div id="issue-detail-actions"></div>
      <pre id="issue-detail-description"></pre>
      <div id="issue-detail-deps" style="display: none;">
        <div id="issue-detail-depends-on"></div>
      </div>
      <div id="issue-detail-blocks-section" style="display: none;">
        <div id="issue-detail-blocks"></div>
      </div>
    </div>
  `;
}

function bead(overrides: Partial<BeadRecord> = {}): BeadRecord {
  return {
    created_at: "2026-05-27T12:00:00Z",
    id: "ga-demo",
    issue_type: "task",
    priority: 2,
    status: "open",
    title: "Demo bead",
    ...overrides,
  };
}

function mockIssue(issue: BeadRecord): void {
  getMock.mockImplementation(async (path: string, init?: { params?: { query?: { status?: string } } }) => {
    if (path === "/v0/city/{cityName}/beads") {
      const status = init?.params?.query?.status;
      return {
        data: { items: status === issue.status ? [issue] : [] },
        error: undefined,
        request: undefined,
        response: undefined,
      };
    }
    if (path === "/v0/city/{cityName}/bead/{id}") {
      return { data: issue, error: undefined, request: undefined, response: undefined };
    }
    if (path === "/v0/city/{cityName}/bead/{id}/deps") {
      return { data: { children: [] }, error: undefined, request: undefined, response: undefined };
    }
    throw new Error(`unexpected GET ${path}`);
  });
}

async function openDetail(issue: BeadRecord): Promise<void> {
  mockIssue(issue);
  await renderIssues();
  document.querySelector<HTMLElement>(".issue-row")?.click();
  await waitFor(() => {
    expect(document.getElementById("issue-detail-id")?.textContent).toBe(issue.id);
  });
}

describe("issue detail timestamps", () => {
  beforeEach(() => {
    getMock.mockReset();
    installDOM();
    vi.spyOn(options, "getOptions").mockResolvedValue({
      agents: ["builder"],
      beads: [],
      fetchedAt: Date.now(),
      mail: [],
      rigs: [],
      sessions: [],
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    document.body.innerHTML = "";
  });

  it("keeps the updated metadata slot immediately after created in source markup", () => {
    const template = document.createElement("template");
    template.innerHTML = indexHTML;

    const ids = [...template.content.querySelectorAll(".issue-detail-meta > span")]
      .map((node) => node.id);

    expect(ids).toEqual([
      "issue-detail-type",
      "issue-detail-owner",
      "issue-detail-created",
      "issue-detail-updated",
    ]);
  });

  it("renders created and updated timestamps with semantic datetimes", async () => {
    const createdAt = "2026-05-27T12:00:00Z";
    const updatedAt = "2026-05-27T12:05:02Z";

    await openDetail(bead({ created_at: createdAt, updated_at: updatedAt }));

    const created = document.getElementById("issue-detail-created");
    const updated = document.getElementById("issue-detail-updated");
    expect(created?.textContent).toContain("Created:");
    expect(created?.querySelector("time")?.getAttribute("datetime")).toBe(createdAt);
    expect(updated?.textContent).toContain("Updated:");
    expect(updated?.querySelector("time")?.getAttribute("datetime")).toBe(updatedAt);
  });

  it("hides updated when updated_at is absent", async () => {
    await openDetail(bead({ updated_at: undefined }));

    const updated = document.getElementById("issue-detail-updated");
    expect(updated?.textContent).toBe("");
    expect(updated?.querySelector("time")).toBeNull();
  });

  it("hides updated inside the one-second threshold", async () => {
    await openDetail(bead({
      created_at: "2026-05-27T12:00:00Z",
      updated_at: "2026-05-27T12:00:01Z",
    }));

    const updated = document.getElementById("issue-detail-updated");
    expect(updated?.textContent).toBe("");
    expect(updated?.querySelector("time")).toBeNull();
  });
});

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
    getMock.mockReset();
    installIssuesDOM();
    window.history.pushState({}, "", "/dashboard?city=test-city");
  });

  afterEach(() => {
    vi.restoreAllMocks();
    window.history.pushState({}, "", "/dashboard");
  });

  it("labels rig buttons by name, filters by prefix, and shows the rig name in the row", async () => {
    getMock.mockImplementation((path: string, opts: { params: { query?: { status?: string } } }) => {
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
    // The "All" tab uses a null sentinel internally and has no data-rig
    // attribute, so a real rig named/prefixed "all" can't collide with it.
    expect(values).toEqual([undefined, "tk", "sl"]);

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

  it("treats a rig prefixed 'all' as an independent filter, not the All sentinel", async () => {
    getMock.mockImplementation((path: string, opts: { params: { query?: { status?: string } } }) => {
      if (path === "/v0/city/{cityName}/beads") {
        if (opts.params.query?.status === "open") {
          return Promise.resolve(ok({
            items: [
              { id: "all-001", title: "rig-named all", priority: 2, status: "open" },
              { id: "tk-001", title: "toolkit bead", priority: 2, status: "open" },
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
        { name: "all-the-things", prefix: "all" },
        { name: "gc-toolkit", prefix: "tk" },
      ],
      sessions: [],
    });

    const { renderIssues } = await import("./issues");
    await renderIssues();

    const rigBtns = Array.from(document.querySelectorAll<HTMLElement>(".rig-btn"));
    // Two distinct buttons whose data-rig values cannot collide:
    // the "All" sentinel button has no data-rig; the "all-the-things"
    // rig button has data-rig="all".
    expect(rigBtns.map((btn) => btn.dataset.rig)).toEqual([undefined, "all", "tk"]);

    // Clicking the "all-the-things" rig (prefix="all") filters to that rig only,
    // not all beads — proving the sentinel and the prefix are independent.
    const allRigBtn = rigBtns.find((btn) => btn.textContent === "all-the-things")!;
    allRigBtn.click();
    expect(
      Array.from(document.querySelectorAll<HTMLElement>(".issue-id")).map((node) => node.textContent),
    ).toEqual(["all-001"]);
  });

  it("falls back to the prefix when a bead's prefix isn't in the rig list", async () => {
    getMock.mockImplementation((path: string, opts: { params: { query?: { status?: string } } }) => {
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
