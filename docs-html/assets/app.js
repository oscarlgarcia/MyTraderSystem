
const lightbox = document.querySelector(".diagram-lightbox");
const lightboxImage = lightbox?.querySelector(".diagram-lightbox-image");
const lightboxCaption = lightbox?.querySelector(".diagram-lightbox-caption");
const lightboxClose = lightbox?.querySelector(".diagram-lightbox-close");

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

lightboxClose?.addEventListener("click", closeDiagramLightbox);
lightbox?.addEventListener("click", (event) => {
  const target = event.target;
  if (target instanceof HTMLElement && target.hasAttribute("data-close-lightbox")) {
    closeDiagramLightbox();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && lightbox && !lightbox.hidden) {
    closeDiagramLightbox();
  }
});
