// Adds a copy-to-clipboard button to every <pre><code> in the
// page, highlights the current sidebar link, and supports
// labelling the language via data-lang on the block.

(function () {
  function copyText(text, button) {
    if (navigator.clipboard) {
      navigator.clipboard.writeText(text).then(() => {
        button.classList.add("copied");
        button.textContent = "Copied";
        setTimeout(() => {
          button.classList.remove("copied");
          button.textContent = "Copy";
        }, 1500);
      });
      return;
    }
    // Fallback for older browsers
    const ta = document.createElement("textarea");
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand("copy");
      button.classList.add("copied");
      button.textContent = "Copied";
      setTimeout(() => {
        button.classList.remove("copied");
        button.textContent = "Copy";
      }, 1500);
    } catch (e) {
      console.error("copy failed", e);
    }
    document.body.removeChild(ta);
  }

  function decorateCodeBlocks() {
    document.querySelectorAll("pre > code").forEach((codeEl) => {
      const pre = codeEl.parentElement;
      if (!pre || pre.parentElement.classList.contains("code-block")) return;

      const wrap = document.createElement("div");
      wrap.className = "code-block";
      pre.parentNode.insertBefore(wrap, pre);
      wrap.appendChild(pre);

      const lang = codeEl.dataset.lang || codeEl.className.replace(/.*language-/, "");
      if (lang && lang !== "language-" && lang !== codeEl.className) {
        const label = document.createElement("span");
        label.className = "lang";
        label.textContent = lang;
        wrap.appendChild(label);
      }

      const btn = document.createElement("button");
      btn.className = "copy";
      btn.type = "button";
      btn.textContent = "Copy";
      btn.addEventListener("click", () => copyText(codeEl.textContent, btn));
      wrap.appendChild(btn);
    });
  }

  function highlightActiveLink() {
    const path = (window.location.pathname.split("/").pop() || "index.html").toLowerCase();
    document.querySelectorAll(".sidebar nav a[data-page]").forEach((a) => {
      if (a.dataset.page.toLowerCase() === path) {
        a.classList.add("active");
      }
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    decorateCodeBlocks();
    highlightActiveLink();
  });
})();
