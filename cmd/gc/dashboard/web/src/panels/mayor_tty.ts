// Spike (gc-lgjze): embed a live ttyd iframe pointed at mayor's tmux
// session. Disposable scaffold — single agent, no auth, no port
// manifest. The operator runs ttyd manually in a separate shell and
// configures the URL here via the input or localStorage.

import { byId } from "../util/dom";

const STORAGE_KEY = "gcMayorTtyUrl";
const DEFAULT_URL = "http://localhost:7681";

function readConfiguredUrl(): string {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY);
    if (stored && stored.trim()) return stored.trim();
  } catch {
    // localStorage may throw in some test environments; fall through
    // to the default.
  }
  return DEFAULT_URL;
}

function writeConfiguredUrl(url: string): void {
  try {
    window.localStorage.setItem(STORAGE_KEY, url);
  } catch {
    // ignore; the in-memory iframe src still updates
  }
}

export function renderMayorTty(): void {
  const iframe = byId<HTMLIFrameElement>("mayor-tty-iframe");
  const input = byId<HTMLInputElement>("mayor-tty-url");
  if (!iframe) return;

  const url = readConfiguredUrl();
  if (input && !input.value) input.value = url;

  // Only update src if it actually changed, to avoid reloading on every
  // renderMayorTty() call (the iframe survives across panel refreshes).
  if (iframe.src !== url) {
    iframe.src = url;
  }
}

export function installMayorTtyInteractions(): void {
  const iframe = byId<HTMLIFrameElement>("mayor-tty-iframe");
  const input = byId<HTMLInputElement>("mayor-tty-url");
  const applyBtn = byId<HTMLButtonElement>("mayor-tty-apply");
  const reloadBtn = byId<HTMLButtonElement>("mayor-tty-reload");

  if (input && !input.value) input.value = readConfiguredUrl();

  applyBtn?.addEventListener("click", () => {
    if (!iframe || !input) return;
    const next = input.value.trim() || DEFAULT_URL;
    writeConfiguredUrl(next);
    iframe.src = next;
  });

  reloadBtn?.addEventListener("click", () => {
    if (!iframe) return;
    const current = iframe.src || readConfiguredUrl();
    // Force reload by toggling src; setting the same value doesn't
    // always trigger a reload in modern browsers.
    iframe.src = "about:blank";
    setTimeout(() => {
      iframe.src = current;
    }, 0);
  });
}
