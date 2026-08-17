// Reveal the hairline scrollbar only while its container is actually
// scrolling; fade it out shortly after the scroll settles.
(function () {
  var timers = new WeakMap();
  document.addEventListener(
    "scroll",
    function (event) {
      var target = event.target;
      if (!(target instanceof Element)) return;
      target.classList.add("is-scrolling");
      var existing = timers.get(target);
      if (existing) clearTimeout(existing);
      timers.set(
        target,
        setTimeout(function () {
          target.classList.remove("is-scrolling");
        }, 700)
      );
    },
    true
  );
})();
