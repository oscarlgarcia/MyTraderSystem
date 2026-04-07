
const navGroups = Array.from(document.querySelectorAll(".nav-group"));
const lightbox = document.querySelector(".diagram-lightbox");
const lightboxImage = lightbox?.querySelector(".diagram-lightbox-image");
const lightboxCaption = lightbox?.querySelector(".diagram-lightbox-caption");
const lightboxClose = lightbox?.querySelector(".diagram-lightbox-close");
const searchRoot = document.querySelector("[data-search-root]");

function closeNavGroups(exceptGroup = null) {
  navGroups.forEach((group) => {
    if (group !== exceptGroup) {
      group.removeAttribute("open");
    }
  });
}

function openDiagramLightbox(trigger) {
  if (!lightbox || !lightboxImage || !lightboxCaption) return;
  const src = trigger.getAttribute("data-diagram-src");
  const alt = trigger.getAttribute("data-diagram-alt") || "Vista ampliada del diagrama";
  if (!src) return;
  lightboxImage.src = src;
  lightboxImage.alt = alt;
  lightboxCaption.textContent = alt;
  lightbox.hidden = false;
  lightbox.setAttribute("aria-hidden", "false");
  document.body.style.overflow = "hidden";
}

function closeDiagramLightbox() {
  if (!lightbox || !lightboxImage) return;
  lightbox.hidden = true;
  lightbox.setAttribute("aria-hidden", "true");
  lightboxImage.src = "";
  document.body.style.overflow = "";
}

document.querySelectorAll(".diagram-zoom-trigger").forEach((trigger) => {
  trigger.addEventListener("click", () => openDiagramLightbox(trigger));
});

navGroups.forEach((group) => {
  group.addEventListener("toggle", () => {
    if (group.open) {
      closeNavGroups(group);
    }
  });
});

lightboxClose?.addEventListener("click", closeDiagramLightbox);
lightbox?.addEventListener("click", (event) => {
  const target = event.target;
  if (target instanceof HTMLElement && target.hasAttribute("data-close-lightbox")) {
    closeDiagramLightbox();
  }
});

document.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof Element)) return;
  if (!target.closest(".top-nav")) {
    closeNavGroups();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeNavGroups();
  }
  if (event.key === "Escape" && lightbox && !lightbox.hidden) {
    closeDiagramLightbox();
  }
});

function normalizeSearchText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\s/-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokenizeQuery(value) {
  return normalizeSearchText(value)
    .split(" ")
    .filter((token) => token.length > 1);
}

function scoreDocument(documentItem, query, tokens, scope) {
  if (scope && documentItem.section !== scope) {
    return null;
  }
  const title = normalizeSearchText(documentItem.title);
  const summary = normalizeSearchText(documentItem.summary);
  const excerpt = normalizeSearchText(documentItem.excerpt);
  const body = normalizeSearchText(documentItem.body);
  const headings = (documentItem.headings || []).map((heading) => normalizeSearchText(heading)).join(" ");
  const haystack = [title, summary, excerpt, body, headings].join(" ");
  if (!query && !scope) {
    return null;
  }
  if (tokens.length && !tokens.every((token) => haystack.includes(token))) {
    return null;
  }
  let score = 0;
  if (query && title.includes(query)) score += title === query ? 140 : 100;
  tokens.forEach((token) => {
    if (title.includes(token)) score += 20;
    if (headings.includes(token)) score += 10;
    if (summary.includes(token)) score += 6;
    if (excerpt.includes(token) || body.includes(token)) score += 2;
  });
  if (scope && documentItem.section === scope) score += 3;
  return score > 0 ? score : null;
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

async function runStaticSearch() {
  if (!searchRoot) return;
  const params = new URLSearchParams(window.location.search);
  const query = params.get("q") || "";
  const scope = params.get("scope") || "";
  const normalizedQuery = normalizeSearchText(query);
  const tokens = tokenizeQuery(query);
  document.querySelectorAll('input[name="q"]').forEach((input) => {
    input.value = query;
  });
  document.querySelectorAll('select[name="scope"]').forEach((select) => {
    select.value = scope;
  });

  const meta = searchRoot.querySelector("[data-search-meta]");
  const resultsContainer = searchRoot.querySelector("[data-search-results]");
  const emptyState = searchRoot.querySelector("[data-search-empty]");
  const indexUrl = searchRoot.getAttribute("data-search-index");
  if (!meta || !resultsContainer || !emptyState || !indexUrl) return;

  const response = await fetch(indexUrl, { cache: "no-store" });
  const documents = await response.json();
  const results = documents
    .map((documentItem) => ({
      documentItem,
      score: scoreDocument(documentItem, normalizedQuery, tokens, scope),
    }))
    .filter((item) => item.score !== null)
    .sort((left, right) => right.score - left.score || left.documentItem.title.localeCompare(right.documentItem.title) || left.documentItem.url.localeCompare(right.documentItem.url))
    .map((item) => item.documentItem);

  if (!query && !scope) {
    meta.textContent = "Introduce una consulta o filtra por seccion para explorar la documentacion.";
    resultsContainer.innerHTML = "";
    emptyState.hidden = false;
    return;
  }

  if (!results.length) {
    meta.textContent = "No hay resultados para la busqueda actual.";
    resultsContainer.innerHTML = "";
    emptyState.hidden = false;
    return;
  }

  emptyState.hidden = true;
  meta.textContent = `${results.length} resultado(s) para "${query || "scope"}"${scope ? ` en ${scope}` : ""}.`;
  resultsContainer.innerHTML = results
    .map((item) => `
      <article class="card search-result-card">
        <h3><a href="${escapeHtml(item.url)}">${escapeHtml(item.title)}</a></h3>
        <p>${escapeHtml(item.excerpt || item.summary)}</p>
        <span class="search-result-meta">${escapeHtml(item.section_name)} · ${escapeHtml(item.url)}</span>
      </article>
    `)
    .join("");
}

runStaticSearch().catch((error) => {
  if (!searchRoot) return;
  const meta = searchRoot.querySelector("[data-search-meta]");
  const emptyState = searchRoot.querySelector("[data-search-empty]");
  if (meta) meta.textContent = "No se pudo cargar el indice de busqueda.";
  if (emptyState) emptyState.hidden = false;
  console.error(error);
});
