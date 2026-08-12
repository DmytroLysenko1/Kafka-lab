window.Walkthrough = (function () {
  "use strict";

  var SVGNS = "http://www.w3.org/2000/svg";
  var WIDTH = 960;
  var PAD_X = 110;
  var TOP = 110;

  var registry = [];
  var keyHandler = null;

  function el(name, attrs, parent) {
    var node = document.createElementNS(SVGNS, name);
    for (var k in attrs) { if (attrs[k] !== undefined && attrs[k] !== null) { node.setAttribute(k, attrs[k]); } }
    if (parent) { parent.appendChild(node); }
    return node;
  }

  function tag(name, className, text, parent) {
    var node = document.createElement(name);
    if (className) { node.className = className; }
    if (text !== undefined) { node.textContent = text; }
    if (parent) { parent.appendChild(node); }
    return node;
  }

  function register(config) { registry.push(config); return config; }
  function list() { return registry.slice(); }
  function get(id) {
    for (var i = 0; i < registry.length; i++) { if (registry[i].id === id) { return registry[i]; } }
    return null;
  }

  function layout(lanes, steps) {
    var x = {};
    var span = lanes.length > 1 ? (WIDTH - 2 * PAD_X) / (lanes.length - 1) : 0;
    lanes.forEach(function (lane, i) { x[lane.id] = PAD_X + i * span; });

    var cursor = TOP;
    steps.forEach(function (step) {
      var half = step.kind === "note" ? 38 : 30;
      step._y = cursor + half;
      cursor = step._y + half + 6;
    });
    return { x: x, height: cursor + 30 };
  }

  function mount(config, selector) {
    var root = document.querySelector(selector || "#app");
    var lanes = config.lanes;
    var steps = config.steps;
    var geo = layout(lanes, steps);

    var current = -1;
    var playing = false;
    var timer = null;
    var raf = null;
    var reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    root.innerHTML = "";
    if (keyHandler) { document.removeEventListener("keydown", keyHandler); keyHandler = null; }

    /* ---------- chrome ---------- */

    var head = tag("div", "topline", undefined, root);
    var titleCol = tag("div", null, undefined, head);
    tag("p", "eyebrow", config.eyebrow, titleCol);
    tag("h1", null, config.title, titleCol);

    var stage = tag("div", "stage", undefined, root);
    var svg = el("svg", { class: "diagram", viewBox: "0 0 " + WIDTH + " " + Math.round(geo.height), role: "img" }, stage);
    var defs = el("defs", {}, svg);
    var marker = el("marker", { id: "wt-head", viewBox: "0 0 10 10", refX: 9, refY: 5,
                                markerWidth: 7, markerHeight: 7, orient: "auto-start-reverse" }, defs);
    el("path", { d: "M 0 0 L 10 5 L 0 10 z", fill: "currentColor" }, marker);
    var lifelines = el("g", {}, svg);
    var actorsG = el("g", {}, svg);
    var arrowsG = el("g", {}, svg);
    var notesG = el("g", {}, svg);
    var packet = el("circle", { class: "packet", r: 8, opacity: 0 }, svg);

    var controls = tag("div", "controls", undefined, root);
    var prevBtn = tag("button", null, "\u2190 Back", controls);
    var playBtn = tag("button", "primary", "Play", controls);
    var nextBtn = tag("button", null, "Next \u2192", controls);
    var dotsBox = tag("div", "dots", undefined, controls);

    var panel = tag("div", "panel", undefined, root);
    tag("p", "hint", "Arrow keys step through, Space plays. " + (config.hint || ""), root);

    /* ---------- static scaffold ---------- */

    lanes.forEach(function (lane) {
      var x = geo.x[lane.id];
      el("line", { x1: x, y1: 78, x2: x, y2: geo.height - 14, class: "lifeline", "stroke-width": 1 }, lifelines);
      el("rect", { x: x - 85, y: 34, width: 170, height: 44, rx: 3, class: "actor-box",
                   "stroke-width": 1, "data-actor": lane.id }, actorsG);
      var label = el("text", { x: x, y: 61, class: "actor-label", "text-anchor": "middle" }, actorsG);
      label.textContent = lane.name;
    });

    /* ---------- drawing ---------- */

    function noteCenter(step) {
      if (Array.isArray(step.at)) { return (geo.x[step.at[0]] + geo.x[step.at[1]]) / 2; }
      return geo.x[step.at];
    }

    function drawNote(step, animate) {
      var cx = noteCenter(step);
      var longest = step.lines.reduce(function (m, s) { return Math.max(m, s.length); }, 0);
      var w = Math.max(160, longest * 7.3 + 28);
      var h = 22 + step.lines.length * 16;
      var g = el("g", { class: animate ? "fade-in" : null }, notesG);
      el("rect", { x: cx - w / 2, y: step._y - h / 2, width: w, height: h, rx: 3,
                   class: "note-box" + (step.warn ? " warn" : ""), "stroke-width": 1 }, g);
      step.lines.forEach(function (line, i) {
        var t = el("text", { x: cx, y: step._y - h / 2 + 20 + i * 16, class: "note-text", "text-anchor": "middle" }, g);
        t.textContent = line;
      });
    }

    function drawArrow(step, index, animate) {
      var x1 = geo.x[step.from], x2 = geo.x[step.to];
      var dir = x2 > x1 ? 1 : -1;
      var g = el("g", { class: animate ? "fade-in" : null }, arrowsG);
      el("line", { x1: x1 + dir * 4, y1: step._y, x2: x2 - dir * 10, y2: step._y,
                   class: "wire" + (step.reply ? " reply" : "") + (step.warn ? " warn" : ""),
                   "stroke-width": 1.6, "marker-end": "url(#wt-head)" }, g);
      var mid = (x1 + x2) / 2;
      var num = el("text", { x: mid, y: step._y - 22, class: "stepnum", "text-anchor": "middle" }, g);
      num.textContent = String(index + 1);
      var lbl = el("text", { x: mid, y: step._y - 8, class: "wire-label", "text-anchor": "middle" }, g);
      lbl.textContent = step.label;
    }

    function highlight(step) {
      var on = {};
      if (step.kind === "msg") { on[step.from] = true; on[step.to] = true; }
      else if (Array.isArray(step.at)) { on[step.at[0]] = true; on[step.at[1]] = true; }
      else { on[step.at] = true; }
      Array.prototype.forEach.call(actorsG.querySelectorAll("rect"), function (r) {
        r.classList.toggle("on", !!on[r.getAttribute("data-actor")]);
      });
    }

    function flyPacket(step, done) {
      if (reduced) { done(); return; }
      var x1 = geo.x[step.from], x2 = geo.x[step.to];
      var start = null;
      var dur = 620;
      packet.setAttribute("cy", step._y);
      packet.setAttribute("opacity", "1");
      function frame(ts) {
        if (start === null) { start = ts; }
        var p = Math.min((ts - start) / dur, 1);
        var eased = p < .5 ? 2 * p * p : 1 - Math.pow(-2 * p + 2, 2) / 2;
        packet.setAttribute("cx", x1 + (x2 - x1) * eased);
        if (p < 1) { raf = requestAnimationFrame(frame); }
        else { packet.setAttribute("opacity", "0"); done(); }
      }
      raf = requestAnimationFrame(frame);
    }

    /* ---------- panel ---------- */

    function renderPanel(index) {
      if (index < 0) { return; }
      var step = steps[index];
      panel.innerHTML = "";
      tag("p", "idx", (index + 1) + " / " + steps.length, panel);
      tag("h2", null, step.t, panel);
      tag("p", null, step.d, panel);
      if (step.r) {
        var risk = tag("p", "risk", undefined, panel);
        tag("b", null, "Watch out. ", risk);
        risk.appendChild(document.createTextNode(step.r));
      }
    }

    function renderDots() {
      dotsBox.innerHTML = "";
      steps.forEach(function (step, i) {
        var b = tag("button", i < current ? "seen" : null, String(i + 1), dotsBox);
        b.setAttribute("aria-label", "Step " + (i + 1));
        if (i === current) { b.setAttribute("aria-current", "true"); }
        b.addEventListener("click", function () { stop(); goTo(i, false); });
      });
    }

    function syncButtons() {
      prevBtn.disabled = current <= 0;
      nextBtn.disabled = current >= steps.length - 1;
      if (playing) { playBtn.textContent = "Pause"; }
      else if (current >= steps.length - 1) { playBtn.textContent = "Replay"; }
      else { playBtn.textContent = "Play"; }
    }

    /* ---------- navigation ---------- */

    function goTo(index, animate, then) {
      if (raf) { cancelAnimationFrame(raf); raf = null; }
      packet.setAttribute("opacity", "0");
      current = index;
      arrowsG.innerHTML = "";
      notesG.innerHTML = "";

      for (var i = 0; i < index; i++) {
        if (steps[i].kind === "msg") { drawArrow(steps[i], i, false); } else { drawNote(steps[i], false); }
      }

      var step = steps[index];
      highlight(step);
      renderPanel(index);
      renderDots();
      syncButtons();

      function settle() {
        if (step.kind === "msg") { drawArrow(step, index, animate); } else { drawNote(step, animate); }
        if (then) { then(); }
      }

      if (animate && step.kind === "msg") { flyPacket(step, settle); } else { settle(); }
    }

    function stop() {
      playing = false;
      if (timer) { clearTimeout(timer); timer = null; }
      if (raf) { cancelAnimationFrame(raf); raf = null; }
      packet.setAttribute("opacity", "0");
      syncButtons();
    }

    function advance() {
      if (!playing) { return; }
      if (current >= steps.length - 1) { stop(); return; }
      goTo(current + 1, true, function () { timer = setTimeout(advance, reduced ? 1400 : 1900); });
    }

    function play() {
      if (playing) { stop(); return; }
      playing = true;
      if (current >= steps.length - 1) { goTo(0, true); }
      syncButtons();
      timer = setTimeout(advance, 900);
    }

    prevBtn.addEventListener("click", function () { stop(); if (current > 0) { goTo(current - 1, false); } });
    nextBtn.addEventListener("click", function () { stop(); if (current < steps.length - 1) { goTo(current + 1, true); } });
    playBtn.addEventListener("click", play);

    keyHandler = function (e) {
      if (e.key === "ArrowRight") { e.preventDefault(); stop(); if (current < steps.length - 1) { goTo(current + 1, true); } }
      else if (e.key === "ArrowLeft") { e.preventDefault(); stop(); if (current > 0) { goTo(current - 1, false); } }
      else if (e.key === " ") { e.preventDefault(); play(); }
    };
    document.addEventListener("keydown", keyHandler);

    goTo(0, false);
  }

  return { register: register, list: list, get: get, mount: mount };
}());
