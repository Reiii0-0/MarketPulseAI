const SVG_NS = "http://www.w3.org/2000/svg";

const state = {
  readiness: null,
  runs: [],
  selectedRunId: null,
  runDetail: null,
  tickers: [],
  selectedTicker: null,
  barsChrono: [],
  predictionsChrono: [],
  predictionDetailsById: new Map(),
  selectedPredictionId: null,
  outcomesSummary: null,
  evidence: null,
  scrubIndex: 0,
  playing: false,
  playTimer: null,
  pollTimer: null,
  previewMode: "live",
  lastError: null,
};

const dom = {
  contractHeaderText: document.getElementById("contractHeaderText"),
  datasetModeBadge: document.getElementById("datasetModeBadge"),
  realMarketGateBadge: document.getElementById("realMarketGateBadge"),
  logicalClockValue: document.getElementById("logicalClockValue"),
  datasetEndValue: document.getElementById("datasetEndValue"),
  readinessBadge: document.getElementById("readinessBadge"),
  syntheticBanner: document.getElementById("syntheticBanner"),
  syntheticBannerTitle: document.getElementById("syntheticBannerTitle"),
  syntheticBannerBody: document.getElementById("syntheticBannerBody"),
  pollToggle: document.getElementById("pollToggle"),
  refreshBtn: document.getElementById("refreshBtn"),
  runSelect: document.getElementById("runSelect"),
  tickerSelect: document.getElementById("tickerSelect"),
  stepBackBtn: document.getElementById("stepBackBtn"),
  playPauseBtn: document.getElementById("playPauseBtn"),
  stepForwardBtn: document.getElementById("stepForwardBtn"),
  timelineSlider: document.getElementById("timelineSlider"),
  speedSelect: document.getElementById("speedSelect"),
  playbackTimestampLabel: document.getElementById("playbackTimestampLabel"),
  statePreviewSelect: document.getElementById("statePreviewSelect"),
  globalAlertBanner: document.getElementById("globalAlertBanner"),
  globalAlertTitle: document.getElementById("globalAlertTitle"),
  globalAlertMessage: document.getElementById("globalAlertMessage"),
  retryBtn: document.getElementById("retryBtn"),
  inputFreshnessBadge: document.getElementById("inputFreshnessBadge"),
  postFitBadge: document.getElementById("postFitBadge"),
  kpiSignal: document.getElementById("kpiSignal"),
  kpiSignalSub: document.getElementById("kpiSignalSub"),
  kpiProbUp: document.getElementById("kpiProbUp"),
  kpiProbNotUp: document.getElementById("kpiProbNotUp"),
  kpiCoverage: document.getElementById("kpiCoverage"),
  kpiAbstainCount: document.getElementById("kpiAbstainCount"),
  kpiAccuracy: document.getElementById("kpiAccuracy"),
  kpiOutcomeBreakdown: document.getElementById("kpiOutcomeBreakdown"),
  overviewSkeleton: document.getElementById("overviewSkeleton"),
  overviewEmpty: document.getElementById("overviewEmpty"),
  overviewEmptyTitle: document.getElementById("overviewEmptyTitle"),
  overviewEmptyBody: document.getElementById("overviewEmptyBody"),
  chartFigure: document.getElementById("chartFigure"),
  priceChartSvg: document.getElementById("priceChartSvg"),
  chartSummaryText: document.getElementById("chartSummaryText"),
  toggleChartTableBtn: document.getElementById("toggleChartTableBtn"),
  chartDataTableWrap: document.getElementById("chartDataTableWrap"),
  chartDataTableBody: document.getElementById("chartDataTableBody"),
  predictionsTableBody: document.getElementById("predictionsTableBody"),
  detailPredictionIdBadge: document.getElementById("detailPredictionIdBadge"),
  detailEmpty: document.getElementById("detailEmpty"),
  detailContent: document.getElementById("detailContent"),
  detBarEnd: document.getElementById("detBarEnd"),
  detDecisionAt: document.getElementById("detDecisionAt"),
  detTargetAt: document.getElementById("detTargetAt"),
  detLogicalAsOf: document.getElementById("detLogicalAsOf"),
  detOutcomeStatus: document.getElementById("detOutcomeStatus"),
  detActualReturn: document.getElementById("detActualReturn"),
  detAttributionSpace: document.getElementById("detAttributionSpace"),
  detFactorList: document.getElementById("detFactorList"),
  linPredId: document.getElementById("linPredId"),
  linRunDataset: document.getElementById("linRunDataset"),
  linContract: document.getElementById("linContract"),
  linBundleModel: document.getElementById("linBundleModel"),
  linFeatureHash: document.getElementById("linFeatureHash"),
  linFitEnd: document.getElementById("linFitEnd"),
  linComputeMs: document.getElementById("linComputeMs"),
  linOutcomeReason: document.getElementById("linOutcomeReason"),
  snapshotFeatureTableBody: document.getElementById("snapshotFeatureTableBody"),
  evidenceNotes: document.getElementById("evidenceNotes"),
  evidenceStatusBadge: document.getElementById("evidenceStatusBadge"),
  realMarketGateCard: document.getElementById("realMarketGateCard"),
  realMarketGateHeadline: document.getElementById("realMarketGateHeadline"),
  realMarketGateDetail: document.getElementById("realMarketGateDetail"),
  evBundleId: document.getElementById("evBundleId"),
  evChampion: document.getElementById("evChampion"),
  evSelectionMetric: document.getElementById("evSelectionMetric"),
  evAbstainBand: document.getElementById("evAbstainBand"),
  evFitInfoEnd: document.getElementById("evFitInfoEnd"),
  evHoldoutWindow: document.getElementById("evHoldoutWindow"),
  evidenceReportContent: document.getElementById("evidenceReportContent"),
  evReportBanner: document.getElementById("evReportBanner"),
  evReportSamples: document.getElementById("evReportSamples"),
  evidenceModelTableBody: document.getElementById("evidenceModelTableBody"),
  evComparatorText: document.getElementById("evComparatorText"),
  evidenceFoldsBody: document.getElementById("evidenceFoldsBody"),
  evidenceCalibrationBody: document.getElementById("evidenceCalibrationBody"),
};

function clearNode(node) {
  while (node.firstChild) {
    node.removeChild(node.firstChild);
  }
}

function makeEl(tag, className, text) {
  const el = document.createElement(tag);
  if (className) el.className = className;
  if (text !== undefined && text !== null) el.textContent = String(text);
  return el;
}

function makeSvg(tag, attrs = {}) {
  const el = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) {
    el.setAttribute(k, String(v));
  }
  return el;
}

function formatUtc(isoValue) {
  if (!isoValue) return "—";
  const d = new Date(isoValue);
  if (Number.isNaN(d.getTime())) return String(isoValue);
  return d.toISOString().replace(".000Z", "Z");
}

function formatShortTime(isoValue) {
  if (!isoValue) return "—";
  const d = new Date(isoValue);
  if (Number.isNaN(d.getTime())) return String(isoValue);
  return d.toISOString().slice(11, 19) + "Z";
}

function fmtNum(val, digits = 4) {
  if (val === null || val === undefined || Number.isNaN(Number(val))) return "—";
  return Number(val).toFixed(digits);
}

function fmtPct(val, digits = 1) {
  if (val === null || val === undefined || Number.isNaN(Number(val))) return "—";
  return `${(Number(val) * 100).toFixed(digits)}%`;
}

function signalBadgeClass(signal) {
  if (signal === "UP") return "badge badge-up";
  if (signal === "NOT_UP") return "badge badge-not-up";
  if (signal === "NO_SIGNAL") return "badge badge-warn";
  return "badge badge-neutral";
}

function outcomeBadgeClass(status) {
  if (status === "realized") return "badge badge-up";
  if (status === "pending") return "badge badge-neutral";
  if (status === "expired") return "badge badge-not-up";
  return "badge badge-warn";
}

async function fetchJson(url) {
  const resp = await fetch(url, { headers: { Accept: "application/json" } });
  let payload = null;
  try {
    payload = await resp.json();
  } catch {
    payload = null;
  }
  return { ok: resp.ok, status: resp.status, payload };
}

function getSelectedRun() {
  return state.runs.find((r) => r.run_id === state.selectedRunId) || null;
}

function getEffectiveBars() {
  if (!state.barsChrono.length) return [];
  const end = Math.min(state.barsChrono.length, Math.max(1, state.scrubIndex + 1));
  return state.barsChrono.slice(0, end);
}

function getEffectivePredictions(visibleBars) {
  if (!visibleBars.length) return [];
  const cutoffIso = visibleBars[visibleBars.length - 1].bar_end;
  const cutoffMs = Date.parse(cutoffIso);
  return state.predictionsChrono.filter((p) => Date.parse(p.bar_end) <= cutoffMs);
}

function renderTopProvenance() {
  const run = getSelectedRun();
  const evidence = state.evidence;
  const readiness = state.readiness;

  const isPreview = state.previewMode !== "live";
  const manifest = evidence?.active_bundle_manifest || null;
  const contractId = manifest?.contract_id || "mp_1m_5m_price_v1";
  const contractVersion = manifest?.contract_version || "1.0.0";
  dom.contractHeaderText.textContent = `Contract: ${contractId} v${contractVersion} · 1m UTC bars, +5m horizon`;

  if (run) {
    dom.datasetModeBadge.textContent = `${run.mode.toUpperCase()} (${run.provenance})`;
    dom.datasetModeBadge.className =
      run.mode === "live" ? "badge badge-up" : "badge badge-neutral";
    dom.logicalClockValue.textContent = formatUtc(run.logical_as_of);
    dom.datasetEndValue.textContent = formatUtc(run.dataset_end_at);
  } else {
    dom.datasetModeBadge.textContent = isPreview ? `PREVIEW (${state.previewMode})` : "No Run Loaded";
    dom.datasetModeBadge.className = "badge badge-neutral";
    dom.logicalClockValue.textContent = "—";
    dom.datasetEndValue.textContent = "—";
  }

  const realValidated = Boolean(evidence?.real_market_validated);
  if (realValidated) {
    dom.realMarketGateBadge.textContent = "Real Market: Validated";
    dom.realMarketGateBadge.className = "badge badge-up";
    dom.syntheticBanner.hidden = true;
  } else {
    dom.realMarketGateBadge.textContent = "Real Market: Unavailable";
    dom.realMarketGateBadge.className = "badge badge-warn";
    dom.syntheticBanner.hidden = false;
    dom.syntheticBannerTitle.textContent = "Synthetic / Smoke Evidence Only";
    dom.syntheticBannerBody.textContent =
      evidence?.notes ||
      "Real-market ~60-session evaluation has not been run yet; fixture metrics verify engineering correctness only.";
  }

  if (state.previewMode === "model_unavailable") {
    dom.readinessBadge.textContent = "Degraded (503 Preview)";
    dom.readinessBadge.className = "badge badge-not-up";
  } else if (readiness) {
    if (readiness.status === "ready") {
      dom.readinessBadge.textContent = "Ready (200)";
      dom.readinessBadge.className = "badge badge-up";
    } else {
      const reasons = (readiness.reasons || []).join(", ") || "unready";
      dom.readinessBadge.textContent = `Degraded (${reasons})`;
      dom.readinessBadge.className = "badge badge-not-up";
    }
  } else {
    dom.readinessBadge.textContent = "Unavailable";
    dom.readinessBadge.className = "badge badge-not-up";
  }
}

function renderRunAndTickerControls() {
  const currentRunId = state.selectedRunId;
  clearNode(dom.runSelect);
  if (!state.runs.length) {
    const opt = makeEl("option", "", "No runs recorded");
    opt.value = "";
    dom.runSelect.appendChild(opt);
    dom.runSelect.disabled = true;
  } else {
    dom.runSelect.disabled = false;
    for (const r of state.runs) {
      const opt = makeEl(
        "option",
        "",
        `${r.run_id} · ${r.dataset_id} (${r.mode}, ${r.predictions_written} preds)`
      );
      opt.value = r.run_id;
      if (r.run_id === currentRunId) opt.selected = true;
      dom.runSelect.appendChild(opt);
    }
  }

  const currentTicker = state.selectedTicker;
  clearNode(dom.tickerSelect);
  if (!state.tickers.length) {
    const opt = makeEl("option", "", "No tickers");
    opt.value = "";
    dom.tickerSelect.appendChild(opt);
    dom.tickerSelect.disabled = true;
  } else {
    dom.tickerSelect.disabled = false;
    for (const t of state.tickers) {
      const opt = makeEl("option", "", t);
      opt.value = t;
      if (t === currentTicker) opt.selected = true;
      dom.tickerSelect.appendChild(opt);
    }
  }

  const maxIdx = Math.max(0, state.barsChrono.length - 1);
  dom.timelineSlider.max = String(maxIdx);
  dom.timelineSlider.value = String(Math.min(state.scrubIndex, maxIdx));
  const disabledPlayback = state.barsChrono.length <= 1 || state.previewMode !== "live";
  dom.timelineSlider.disabled = disabledPlayback;
  dom.stepBackBtn.disabled = disabledPlayback || state.scrubIndex <= 0;
  dom.stepForwardBtn.disabled = disabledPlayback || state.scrubIndex >= maxIdx;
  dom.playPauseBtn.disabled = disabledPlayback;
  dom.playPauseBtn.textContent = state.playing ? "Pause" : "Play";

  const visibleBars = getEffectiveBars();
  dom.playbackTimestampLabel.textContent = visibleBars.length
    ? `${formatUtc(visibleBars[visibleBars.length - 1].bar_end)} (${visibleBars.length}/${state.barsChrono.length} bars)`
    : "—";
}

function renderAlertBanner() {
  const mode = state.previewMode;
  if (mode === "error") {
    dom.globalAlertBanner.hidden = false;
    dom.globalAlertBanner.className = "alert-banner is-error";
    dom.globalAlertTitle.textContent = "API Read Failure (Simulated Preview)";
    dom.globalAlertMessage.textContent =
      "Could not fetch run or outcome data from the read API. Internal details are hidden; verify PostgreSQL and API health then retry.";
    return;
  }
  if (mode === "model_unavailable") {
    dom.globalAlertBanner.hidden = false;
    dom.globalAlertBanner.className = "alert-banner is-error";
    dom.globalAlertTitle.textContent = "Prediction Readiness Degraded (HTTP 503 Preview)";
    dom.globalAlertMessage.textContent =
      "Verified model bundle is missing or failed contract check. Existing finalized bars and prior evidence remain readable.";
    return;
  }
  if (mode === "stale") {
    dom.globalAlertBanner.hidden = false;
    dom.globalAlertBanner.className = "alert-banner";
    dom.globalAlertTitle.textContent = "Stale Input / Expired Backlog Warning (Preview)";
    dom.globalAlertMessage.textContent =
      "Bar arrival fell past +5m horizon maturity before worker evaluation; late snapshots are marked expired and excluded from timely accuracy.";
    return;
  }

  if (state.lastError) {
    dom.globalAlertBanner.hidden = false;
    dom.globalAlertBanner.className = "alert-banner is-error";
    dom.globalAlertTitle.textContent = "Read API Request Failed";
    dom.globalAlertMessage.textContent = state.lastError;
    return;
  }

  if (state.readiness && state.readiness.status !== "ready") {
    const reasons = (state.readiness.reasons || []).join(", ") || "dependencies not ready";
    dom.globalAlertBanner.hidden = false;
    dom.globalAlertBanner.className = "alert-banner";
    dom.globalAlertTitle.textContent = "System Readiness Degraded (HTTP 503)";
    dom.globalAlertMessage.textContent = `Readiness check reported: ${reasons}. Liveness remains active and finalized read-only artifacts remain inspectable.`;
    return;
  }

  dom.globalAlertBanner.hidden = true;
}

function renderOverview() {
  const mode = state.previewMode;
  dom.overviewSkeleton.hidden = mode !== "loading";

  if (mode === "loading") {
    dom.chartFigure.hidden = true;
    dom.overviewEmpty.hidden = true;
    dom.inputFreshnessBadge.textContent = "Freshness: Loading…";
    dom.inputFreshnessBadge.className = "badge badge-neutral";
    dom.postFitBadge.textContent = "Window Check: Loading…";
    return;
  }

  const visibleBars = mode === "empty" || mode === "error" ? [] : getEffectiveBars();
  const visiblePreds = mode === "empty" || mode === "error" ? [] : getEffectivePredictions(visibleBars);
  const run = getSelectedRun();

  // Window check & freshness badges
  const servesPostFit = state.runDetail?.serves_post_fit_window;
  if (servesPostFit === true) {
    dom.postFitBadge.textContent = "Post-Fit Window: Valid (Holdout)";
    dom.postFitBadge.className = "badge badge-up";
  } else if (servesPostFit === false) {
    dom.postFitBadge.textContent = "In-Sample Warning (Before Fit End)";
    dom.postFitBadge.className = "badge badge-warn";
  } else {
    dom.postFitBadge.textContent = "Window Check: Unknown";
    dom.postFitBadge.className = "badge badge-neutral";
  }

  if (mode === "stale" || (run && run.expired_backlog_count > 0)) {
    const expCount = mode === "stale" ? 3 : run?.expired_backlog_count || 0;
    dom.inputFreshnessBadge.textContent = `Stale / Expired Backlog: ${expCount}`;
    dom.inputFreshnessBadge.className = "badge badge-not-up";
  } else if (!visibleBars.length) {
    dom.inputFreshnessBadge.textContent = "Freshness: No Bars";
    dom.inputFreshnessBadge.className = "badge badge-warn";
  } else {
    dom.inputFreshnessBadge.textContent = `Freshness: Canonical (${formatShortTime(visibleBars[visibleBars.length - 1].bar_end)})`;
    dom.inputFreshnessBadge.className = "badge badge-up";
  }

  if (!visibleBars.length) {
    dom.chartFigure.hidden = true;
    dom.overviewEmpty.hidden = false;
    dom.overviewEmptyTitle.textContent =
      mode === "error" ? "Bars Unavailable Due to Read Error" : "No Finalized Bars or Predictions Available";
    dom.overviewEmptyBody.textContent =
      mode === "error"
        ? "Retry once the API and PostgreSQL database respond to /api/v1/health/ready."
        : "Run the replay producer, consumer, and single prediction worker (or select another run/ticker) to inspect canonical 1m bars.";

    dom.kpiSignal.textContent = "—";
    dom.kpiSignal.className = "metric-value";
    dom.kpiProbUp.textContent = "—";
    dom.kpiProbNotUp.textContent = "P(NOT_UP): —";
    dom.kpiCoverage.textContent = "—";
    dom.kpiAbstainCount.textContent = "NO_SIGNAL: —";
    dom.kpiAccuracy.textContent = "—";
    dom.kpiOutcomeBreakdown.textContent = "Realized: 0 · Pending: 0 · Unavail: 0 · Exp: 0";
    return;
  }

  dom.overviewEmpty.hidden = true;
  dom.chartFigure.hidden = false;

  const latestPred = visiblePreds.length ? visiblePreds[visiblePreds.length - 1] : null;
  if (latestPred) {
    dom.kpiSignal.textContent = `${latestPred.signal_label} (${latestPred.confidence_tier})`;
    dom.kpiSignal.className = `metric-value ${
      latestPred.signal_label === "UP"
        ? "mono"
        : ""
    }`;
    dom.kpiSignalSub.textContent =
      latestPred.signal_label === "NO_SIGNAL"
        ? `Abstained near 0.50 · underlying class: ${latestPred.predicted_direction}`
        : `Binary contract class: ${latestPred.predicted_direction} · status: ${latestPred.outcome_status}`;
    dom.kpiProbUp.textContent = fmtPct(latestPred.probability_up, 2);
    dom.kpiProbNotUp.textContent = `P(NOT_UP): ${fmtPct(latestPred.probability_not_up, 2)}`;
  } else {
    dom.kpiSignal.textContent = mode === "model_unavailable" ? "UNAVAILABLE" : "No Signal Yet";
    dom.kpiSignalSub.textContent =
      mode === "model_unavailable"
        ? "Model bundle missing — bars finalized without predictions"
        : "Warm-up requires 20 contiguous 1m bars before scoring";
    dom.kpiProbUp.textContent = "—";
    dom.kpiProbNotUp.textContent = "P(NOT_UP): —";
  }

  const totalP = visiblePreds.length;
  const noSig = visiblePreds.filter((p) => p.signal_label === "NO_SIGNAL").length;
  const signaledP = totalP - noSig;
  const realized = visiblePreds.filter((p) => p.outcome_status === "realized").length;
  const pending = visiblePreds.filter((p) => p.outcome_status === "pending").length;
  const unavail = visiblePreds.filter((p) => p.outcome_status === "unavailable").length;
  const expired = visiblePreds.filter((p) => p.outcome_status === "expired").length;
  const timelySignaledRealized = visiblePreds.filter(
    (p) =>
      p.outcome_status === "realized" &&
      p.timeliness_status === "timely" &&
      (p.signal_label === "UP" || p.signal_label === "NOT_UP")
  );
  const timelyCorrect = timelySignaledRealized.filter((p) => p.was_correct === true).length;

  dom.kpiCoverage.textContent = totalP > 0 ? `${fmtPct(signaledP / totalP, 1)} (${signaledP}/${totalP})` : "—";
  dom.kpiAbstainCount.textContent = `NO_SIGNAL abstained: ${noSig}`;
  dom.kpiAccuracy.textContent =
    timelySignaledRealized.length > 0
      ? `${fmtPct(timelyCorrect / timelySignaledRealized.length, 1)} (${timelyCorrect}/${timelySignaledRealized.length})`
      : "Pending maturity";
  dom.kpiOutcomeBreakdown.textContent = `Realized: ${realized} · Pending: ${pending} · Unavail: ${unavail} · Exp: ${expired}`;

  renderPriceChart(visibleBars, visiblePreds);
  renderChartDataTable(visibleBars, visiblePreds);
}

function renderPriceChart(bars, preds) {
  const svg = dom.priceChartSvg;
  clearNode(svg);

  const width = 920;
  const height = 300;
  const padL = 62;
  const padR = 24;
  const padT = 20;
  const padB = 34;
  const plotW = width - padL - padR;
  const plotH = height - padT - padB;

  const prices = bars.flatMap((b) => [Number(b.high), Number(b.low), Number(b.close)]);
  let minP = Math.min(...prices);
  let maxP = Math.max(...prices);
  if (maxP - minP < 1e-6) {
    minP -= 0.5;
    maxP += 0.5;
  } else {
    const margin = (maxP - minP) * 0.08;
    minP -= margin;
    maxP += margin;
  }

  const xAt = (idx) =>
    bars.length === 1 ? padL + plotW / 2 : padL + (idx / (bars.length - 1)) * plotW;
  const yAt = (price) => padT + ((maxP - price) / (maxP - minP)) * plotH;

  // Horizontal grid + Y labels
  for (let i = 0; i <= 4; i++) {
    const frac = i / 4;
    const y = padT + frac * plotH;
    const pVal = maxP - frac * (maxP - minP);
    svg.appendChild(
      makeSvg("line", {
        x1: padL,
        y1: y.toFixed(1),
        x2: width - padR,
        y2: y.toFixed(1),
        stroke: "#e2e8f0",
        "stroke-width": 1,
      })
    );
    const lbl = makeSvg("text", {
      x: padL - 8,
      y: (y + 4).toFixed(1),
      "text-anchor": "end",
      fill: "#475569",
      "font-size": 11,
      "font-family": "ui-monospace, monospace",
    });
    lbl.textContent = pVal.toFixed(2);
    svg.appendChild(lbl);
  }

  // Highlight currently inspected prediction bar_end and target_at if within chart
  const barIdxByIso = new Map(bars.map((b, i) => [formatUtc(b.bar_end), i]));
  const selectedPred = preds.find((p) => p.prediction_id === state.selectedPredictionId);
  if (selectedPred) {
    const decIdx = barIdxByIso.get(formatUtc(selectedPred.bar_end));
    if (decIdx !== undefined) {
      const dx = xAt(decIdx);
      svg.appendChild(
        makeSvg("line", {
          x1: dx.toFixed(1),
          y1: padT,
          x2: dx.toFixed(1),
          y2: height - padB,
          stroke: "#0f172a",
          "stroke-width": 1.5,
          "stroke-dasharray": "4 3",
        })
      );
      const tgtIdx = barIdxByIso.get(formatUtc(selectedPred.target_at));
      if (tgtIdx !== undefined) {
        const tx = xAt(tgtIdx);
        svg.appendChild(
          makeSvg("rect", {
            x: Math.min(dx, tx).toFixed(1),
            y: padT,
            width: Math.max(2, Math.abs(tx - dx)).toFixed(1),
            height: plotH,
            fill: "#3730a3",
            opacity: "0.08",
          })
        );
      }
    }
  }

  // Subtle high-low wicks per bar
  bars.forEach((b, i) => {
    const x = xAt(i);
    svg.appendChild(
      makeSvg("line", {
        x1: x.toFixed(1),
        y1: yAt(Number(b.high)).toFixed(1),
        x2: x.toFixed(1),
        y2: yAt(Number(b.low)).toFixed(1),
        stroke: "#cbd5e1",
        "stroke-width": 1.5,
      })
    );
  });

  // Close polyline
  const polyPoints = bars
    .map((b, i) => `${xAt(i).toFixed(1)},${yAt(Number(b.close)).toFixed(1)}`)
    .join(" ");
  svg.appendChild(
    makeSvg("polyline", {
      points: polyPoints,
      fill: "none",
      stroke: "#1d4ed8",
      "stroke-width": 2.2,
      "stroke-linejoin": "round",
    })
  );

  // Prediction markers
  const predByIso = new Map(preds.map((p) => [formatUtc(p.bar_end), p]));
  bars.forEach((b, i) => {
    const p = predByIso.get(formatUtc(b.bar_end));
    if (!p) return;
    const cx = xAt(i);
    const cy = yAt(Number(b.close));
    let color = "#92400e";
    if (p.signal_label === "UP") color = "#047857";
    if (p.signal_label === "NOT_UP") color = "#b91c1c";
    const isSel = p.prediction_id === state.selectedPredictionId;
    svg.appendChild(
      makeSvg("circle", {
        cx: cx.toFixed(1),
        cy: cy.toFixed(1),
        r: isSel ? 6 : 4.2,
        fill: color,
        stroke: isSel ? "#0f172a" : "#ffffff",
        "stroke-width": isSel ? 2 : 1.2,
      })
    );
  });

  // X-axis first / middle / last UTC timestamps
  const xTickIndices =
    bars.length <= 2 ? [0, bars.length - 1] : [0, Math.floor((bars.length - 1) / 2), bars.length - 1];
  for (const idx of new Set(xTickIndices)) {
    const lbl = makeSvg("text", {
      x: xAt(idx).toFixed(1),
      y: height - 10,
      "text-anchor": idx === 0 ? "start" : idx === bars.length - 1 ? "end" : "middle",
      fill: "#475569",
      "font-size": 11,
      "font-family": "ui-monospace, monospace",
    });
    lbl.textContent = formatShortTime(bars[idx].bar_end);
    svg.appendChild(lbl);
  }

  const firstClose = Number(bars[0].close);
  const lastClose = Number(bars[bars.length - 1].close);
  const deltaPct = firstClose > 0 ? ((lastClose - firstClose) / firstClose) * 100 : 0;
  dom.chartSummaryText.textContent = `${state.selectedTicker || "Ticker"}: ${bars.length} finalized 1m UTC bars from ${formatUtc(
    bars[0].bar_end
  )} to ${formatUtc(bars[bars.length - 1].bar_end)}. Close moved from ${firstClose.toFixed(
    2
  )} to ${lastClose.toFixed(2)} (${deltaPct >= 0 ? "+" : ""}${deltaPct.toFixed(
    2
  )}%), with ${preds.length} immutable prediction markers shown.`;
}

function renderChartDataTable(bars, preds) {
  clearNode(dom.chartDataTableBody);
  const predMap = new Map(preds.map((p) => [formatUtc(p.bar_end), p]));
  const recentFirst = [...bars].reverse().slice(0, 60);
  for (const b of recentFirst) {
    const tr = document.createElement("tr");
    const p = predMap.get(formatUtc(b.bar_end));
    tr.appendChild(makeEl("td", "mono", formatUtc(b.bar_end)));
    tr.appendChild(makeEl("td", "mono", fmtNum(b.open, 2)));
    tr.appendChild(makeEl("td", "mono", fmtNum(b.high, 2)));
    tr.appendChild(makeEl("td", "mono", fmtNum(b.low, 2)));
    tr.appendChild(makeEl("td", "mono", fmtNum(b.close, 2)));
    tr.appendChild(makeEl("td", "mono", String(b.volume)));
    tr.appendChild(makeEl("td", "mono", p ? `${p.signal_label} (${fmtPct(p.probability_up, 1)})` : "—"));
    dom.chartDataTableBody.appendChild(tr);
  }
}

async function selectPrediction(predictionId) {
  state.selectedPredictionId = predictionId;
  renderPredictionsArea();
  renderOverview();

  if (!predictionId) return;
  if (!state.predictionDetailsById.has(predictionId)) {
    const res = await fetchJson(`/api/v1/predictions/${encodeURIComponent(predictionId)}`);
    if (res.ok && res.payload) {
      state.predictionDetailsById.set(predictionId, res.payload);
      renderPredictionsArea();
    }
  }
}

function renderPredictionsArea() {
  const mode = state.previewMode;
  const visibleBars = mode === "empty" || mode === "error" || mode === "loading" ? [] : getEffectiveBars();
  const visiblePreds = getEffectivePredictions(visibleBars);

  if (
    !state.selectedPredictionId ||
    !visiblePreds.some((p) => p.prediction_id === state.selectedPredictionId)
  ) {
    state.selectedPredictionId = visiblePreds.length
      ? visiblePreds[visiblePreds.length - 1].prediction_id
      : null;
  }

  clearNode(dom.predictionsTableBody);
  const descPreds = [...visiblePreds].reverse();
  if (!descPreds.length) {
    const tr = document.createElement("tr");
    const td = makeEl("td", "", "No predictions recorded inside current timeline window.");
    td.colSpan = 6;
    tr.appendChild(td);
    dom.predictionsTableBody.appendChild(tr);
  } else {
    for (const p of descPreds) {
      const tr = document.createElement("tr");
      tr.className = "pred-row";
      tr.tabIndex = 0;
      const isSelected = p.prediction_id === state.selectedPredictionId;
      tr.setAttribute("aria-selected", isSelected ? "true" : "false");

      tr.appendChild(makeEl("td", "mono", formatShortTime(p.bar_end)));

      const sigTd = document.createElement("td");
      sigTd.appendChild(makeEl("span", signalBadgeClass(p.signal_label), p.signal_label));
      tr.appendChild(sigTd);

      tr.appendChild(makeEl("td", "mono", fmtPct(p.probability_up, 1)));
      tr.appendChild(makeEl("td", "mono", p.timeliness_status));

      const outTd = document.createElement("td");
      const outLabel = p.actual_direction ? `${p.outcome_status}: ${p.actual_direction}` : p.outcome_status;
      outTd.appendChild(makeEl("span", outcomeBadgeClass(p.outcome_status), outLabel));
      tr.appendChild(outTd);

      tr.appendChild(
        makeEl("td", "mono", p.return_5m !== null && p.return_5m !== undefined ? fmtPct(p.return_5m, 3) : "—")
      );

      tr.addEventListener("click", () => selectPrediction(p.prediction_id));
      tr.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter" || ev.key === " ") {
          ev.preventDefault();
          selectPrediction(p.prediction_id);
        }
      });

      dom.predictionsTableBody.appendChild(tr);
    }
  }

  const active = visiblePreds.find((p) => p.prediction_id === state.selectedPredictionId) || null;
  if (!active) {
    dom.detailPredictionIdBadge.textContent = "Prediction: —";
    dom.detailEmpty.hidden = false;
    dom.detailContent.hidden = true;
    return;
  }

  dom.detailEmpty.hidden = true;
  dom.detailContent.hidden = false;
  dom.detailPredictionIdBadge.textContent = `Prediction: ${active.prediction_id.slice(0, 8)}…`;

  dom.detBarEnd.textContent = formatUtc(active.bar_end);
  dom.detDecisionAt.textContent = formatUtc(active.decision_at);
  dom.detTargetAt.textContent = formatUtc(active.target_at);
  dom.detLogicalAsOf.textContent = formatUtc(active.logical_as_of);

  clearNode(dom.detOutcomeStatus);
  dom.detOutcomeStatus.appendChild(
    makeEl("span", outcomeBadgeClass(active.outcome_status), active.outcome_status.toUpperCase())
  );
  dom.detActualReturn.textContent =
    active.actual_direction !== null && active.return_5m !== null
      ? `${active.actual_direction} (${fmtPct(active.return_5m, 3)})`
      : active.outcome_reason || "pending";

  dom.detAttributionSpace.textContent = active.attribution_space;
  clearNode(dom.detFactorList);
  const factors = active.factors || [];
  if (!factors.length) {
    dom.detFactorList.appendChild(makeEl("li", "methodology-note", "No feature contributions stored (constant baseline or abstained baseline)."));
  } else {
    const maxAbs = Math.max(...factors.map((f) => Math.abs(Number(f.shap_value || 0))), 1e-6);
    for (const f of factors) {
      const li = document.createElement("li");
      li.className = "factor-item";
      li.appendChild(makeEl("span", "mono", f.feature));
      const track = makeEl("div", "factor-bar-track");
      const val = Number(f.shap_value || 0);
      const fill = makeEl("div", `factor-bar-fill ${val >= 0 ? "factor-bar-pos" : "factor-bar-neg"}`);
      fill.style.width = `${Math.min(100, Math.max(4, (Math.abs(val) / maxAbs) * 100)).toFixed(1)}%`;
      track.appendChild(fill);
      li.appendChild(track);
      li.appendChild(makeEl("span", "mono", `${val >= 0 ? "+" : ""}${val.toFixed(4)} (${f.direction})`));
      dom.detFactorList.appendChild(li);
    }
  }

  const detail = state.predictionDetailsById.get(active.prediction_id) || null;
  dom.linPredId.textContent = active.prediction_id;
  dom.linRunDataset.textContent = `${active.run_id || "—"} · ${active.dataset_id} (${active.run_mode})`;
  dom.linContract.textContent = `${active.contract_id} v${detail?.contract_version || "1.0.0"}`;
  dom.linBundleModel.textContent = `${active.bundle_id || "—"} · ${active.champion_model} (${active.model_version})`;
  dom.linFeatureHash.textContent = active.feature_hash;
  dom.linFitEnd.textContent = detail?.fit_information_end ? formatUtc(detail.fit_information_end) : "—";
  dom.linComputeMs.textContent = `${fmtNum(active.compute_duration_ms, 2)} ms`;
  dom.linOutcomeReason.textContent = active.outcome_reason || "—";

  clearNode(dom.snapshotFeatureTableBody);
  const feats = detail?.snapshot_features || null;
  if (!feats || !Object.keys(feats).length) {
    const tr = document.createElement("tr");
    const td = makeEl(
      "td",
      "methodology-note",
      "Fetching frozen feature snapshot from /api/v1/predictions/{id}…"
    );
    td.colSpan = 2;
    tr.appendChild(td);
    dom.snapshotFeatureTableBody.appendChild(tr);
  } else {
    for (const [fname, fval] of Object.entries(feats)) {
      const tr = document.createElement("tr");
      tr.appendChild(makeEl("td", "mono", fname));
      tr.appendChild(makeEl("td", "mono", typeof fval === "number" ? fmtNum(fval, 6) : String(fval)));
      dom.snapshotFeatureTableBody.appendChild(tr);
    }
  }
}

function renderEvidenceSection() {
  const ev = state.evidence;
  if (!ev) {
    dom.evidenceStatusBadge.textContent = "Unavailable";
    dom.evidenceStatusBadge.className = "badge badge-not-up";
    dom.realMarketGateHeadline.textContent = "Evidence endpoint did not return data.";
    dom.evidenceReportContent.hidden = true;
    return;
  }

  dom.evidenceNotes.textContent = ev.notes;
  const realOk = Boolean(ev.real_market_validated && ev.real_market_report);
  if (realOk) {
    dom.evidenceStatusBadge.textContent = "Real Market Validated";
    dom.evidenceStatusBadge.className = "badge badge-up";
    dom.realMarketGateCard.className = "gate-card is-validated";
    dom.realMarketGateHeadline.textContent = `Real Market Dataset Verified (${ev.real_market_report.dataset_id})`;
    dom.realMarketGateDetail.textContent =
      "Chronological walk-forward evaluation and holdout testing verified on ~60 historical market sessions.";
  } else {
    dom.evidenceStatusBadge.textContent = "Real Market: Unavailable (Smoke Only)";
    dom.evidenceStatusBadge.className = "badge badge-warn";
    dom.realMarketGateCard.className = "gate-card is-unavailable";
    dom.realMarketGateHeadline.textContent = "Real-Market Validation: UNAVAILABLE";
    dom.realMarketGateDetail.textContent =
      "No ~60-session real historical 1m market evaluation is published yet. Only the synthetic smoke fixture report below is available and must not be read as market edge.";
  }

  const manifest = ev.active_bundle_manifest;
  if (manifest) {
    dom.evBundleId.textContent = manifest.bundle_id || "—";
    dom.evChampion.textContent = `${manifest.champion_model || "—"} (${manifest.model_version || "—"})`;
    dom.evSelectionMetric.textContent = manifest.selection_metric || "brier_score";
    dom.evAbstainBand.textContent = `delta = ${manifest.abstain_band_delta ?? 0}`;
    dom.evFitInfoEnd.textContent = formatUtc(manifest.fit_information_end);
    dom.evHoldoutWindow.textContent = manifest.cutoffs
      ? `${formatUtc(manifest.cutoffs.holdout_start)} -> ${formatUtc(manifest.cutoffs.holdout_end)}`
      : "—";
  }

  const report = ev.real_market_report || (ev.synthetic_smoke_reports && ev.synthetic_smoke_reports[0]) || null;
  if (!report) {
    dom.evidenceReportContent.hidden = true;
    return;
  }

  dom.evidenceReportContent.hidden = false;
  dom.evReportBanner.textContent = report.is_real_market_data
    ? `Real-Market Evaluation Report · ${report.dataset_id}`
    : `${report.warning_banner || "SYNTHETIC FIXTURE REPORT ONLY"} (${report.dataset_id})`;
  const cutoffs = report.cutoffs || {};
  dom.evReportSamples.textContent = `pre-holdout n=${cutoffs.pre_holdout_samples ?? "?"} · holdout n=${
    cutoffs.holdout_samples ?? "?"
  } · purge=${cutoffs.purge_seconds ?? 300}s`;

  clearNode(dom.evidenceModelTableBody);
  const holdout = report.holdout_summary || {};
  const valSummary = report.validation_summary || {};
  for (const modelKey of Object.keys(holdout)) {
    const h = holdout[modelKey];
    const vCal = valSummary[modelKey]?.calibrated || valSummary[modelKey]?.raw || null;
    const isChamp = modelKey === report.champion_model;
    const tr = document.createElement("tr");
    tr.appendChild(makeEl("td", "mono", `${modelKey}${isChamp ? " (champion)" : ""} [holdout]`));
    tr.appendChild(makeEl("td", "mono", String(h.n_samples ?? "—")));
    tr.appendChild(makeEl("td", "mono", fmtNum(h.brier_score, 4)));
    tr.appendChild(makeEl("td", "mono", fmtNum(h.log_loss, 4)));
    tr.appendChild(makeEl("td", "mono", fmtNum(h.ece, 4)));
    tr.appendChild(makeEl("td", "mono", fmtPct(h.balanced_accuracy, 1)));
    tr.appendChild(makeEl("td", "mono", fmtPct(h.coverage, 1)));
    tr.appendChild(makeEl("td", "mono", fmtPct(h.covered_error_rate, 1)));
    dom.evidenceModelTableBody.appendChild(tr);

    if (vCal) {
      const trV = document.createElement("tr");
      trV.appendChild(makeEl("td", "mono", `${modelKey} [oof-val]`));
      trV.appendChild(makeEl("td", "mono", String(vCal.n_samples ?? "—")));
      trV.appendChild(makeEl("td", "mono", fmtNum(vCal.brier_score, 4)));
      trV.appendChild(makeEl("td", "mono", fmtNum(vCal.log_loss, 4)));
      trV.appendChild(makeEl("td", "mono", fmtNum(vCal.ece, 4)));
      trV.appendChild(makeEl("td", "mono", fmtPct(vCal.balanced_accuracy, 1)));
      trV.appendChild(makeEl("td", "mono", fmtPct(vCal.coverage, 1)));
      trV.appendChild(makeEl("td", "mono", fmtPct(vCal.covered_error_rate, 1)));
      dom.evidenceModelTableBody.appendChild(trV);
    }
  }

  const rw = report.comparators?.random_walk_price;
  dom.evComparatorText.textContent = rw
    ? `Random walk comparator (${rw.definition}): n=${rw.n_samples}, hit_rate=${fmtPct(
        rw.hit_rate,
        1
      )}, balanced_accuracy=${fmtPct(rw.balanced_accuracy, 1)}.`
    : "No random walk comparator recorded.";

  clearNode(dom.evidenceFoldsBody);
  const folds = cutoffs.walk_forward_folds || [];
  const perFold = report.per_fold || [];
  folds.forEach((f, idx) => {
    const pf = perFold[idx] || {};
    const tr = document.createElement("tr");
    tr.appendChild(makeEl("td", "mono", `Fold ${f.fold}`));
    tr.appendChild(makeEl("td", "mono", `${formatShortTime(f.train_start)}..${formatShortTime(f.train_end)}`));
    tr.appendChild(makeEl("td", "mono", `${formatShortTime(f.val_start)}..${formatShortTime(f.val_end)}`));
    tr.appendChild(makeEl("td", "mono", `${pf.train_samples ?? "?"} / ${pf.val_samples ?? "?"}`));
    dom.evidenceFoldsBody.appendChild(tr);
  });

  clearNode(dom.evidenceCalibrationBody);
  const champHoldout = holdout[report.champion_model] || null;
  const bins = champHoldout?.calibration_bins || [];
  for (const b of bins) {
    const tr = document.createElement("tr");
    tr.appendChild(makeEl("td", "mono", `[${b.bin_range[0].toFixed(1)}, ${b.bin_range[1].toFixed(1)})`));
    tr.appendChild(makeEl("td", "mono", String(b.count)));
    tr.appendChild(makeEl("td", "mono", b.mean_pred_prob !== null ? fmtNum(b.mean_pred_prob, 3) : "—"));
    tr.appendChild(makeEl("td", "mono", b.empirical_up_rate !== null ? fmtPct(b.empirical_up_rate, 1) : "—"));
    dom.evidenceCalibrationBody.appendChild(tr);
  }
}

function renderAll() {
  renderTopProvenance();
  renderRunAndTickerControls();
  renderAlertBanner();
  renderOverview();
  renderPredictionsArea();
  renderEvidenceSection();
}

async function loadRunScopedData() {
  const run = getSelectedRun();
  if (!run) {
    state.runDetail = null;
    state.tickers = [];
    state.selectedTicker = null;
    state.barsChrono = [];
    state.predictionsChrono = [];
    state.outcomesSummary = null;
    state.scrubIndex = 0;
    renderAll();
    return;
  }

  const [detailRes, tickersRes] = await Promise.all([
    fetchJson(`/api/v1/runs/${encodeURIComponent(run.run_id)}`),
    fetchJson(`/api/v1/tickers?run_id=${encodeURIComponent(run.run_id)}&dataset_id=${encodeURIComponent(run.dataset_id)}`),
  ]);

  if (detailRes.ok) state.runDetail = detailRes.payload;
  const tickers = tickersRes.ok && tickersRes.payload?.tickers ? tickersRes.payload.tickers : [];
  state.tickers = tickers;
  if (!tickers.includes(state.selectedTicker)) {
    state.selectedTicker = tickers[0] || null;
  }

  if (!state.selectedTicker) {
    state.barsChrono = [];
    state.predictionsChrono = [];
    state.outcomesSummary = null;
    state.scrubIndex = 0;
    renderAll();
    return;
  }

  const prevAtEnd = state.scrubIndex >= Math.max(0, state.barsChrono.length - 1);
  const tickerParam = encodeURIComponent(state.selectedTicker);
  const runParam = encodeURIComponent(run.run_id);
  const dsParam = encodeURIComponent(run.dataset_id);

  const [barsRes, predsRes] = await Promise.all([
    fetchJson(`/api/v1/bars?run_id=${runParam}&dataset_id=${dsParam}&ticker=${tickerParam}&limit=300`),
    fetchJson(`/api/v1/predictions?run_id=${runParam}&dataset_id=${dsParam}&ticker=${tickerParam}&limit=200`),
  ]);

  if (!barsRes.ok || !predsRes.ok) {
    state.lastError =
      barsRes.payload?.detail?.message ||
      predsRes.payload?.detail?.message ||
      "Unable to query finalized bars or predictions.";
  } else {
    state.lastError = null;
  }

  const rawBars = barsRes.ok && barsRes.payload?.bars ? barsRes.payload.bars : [];
  state.barsChrono = [...rawBars].reverse();
  const rawPreds = predsRes.ok && predsRes.payload?.predictions ? predsRes.payload.predictions : [];
  state.predictionsChrono = [...rawPreds].reverse();

  if (prevAtEnd || state.scrubIndex >= state.barsChrono.length) {
    state.scrubIndex = Math.max(0, state.barsChrono.length - 1);
  }

  renderAll();
  const activePreds = getEffectivePredictions(getEffectiveBars());
  if (state.selectedPredictionId && activePreds.some((p) => p.prediction_id === state.selectedPredictionId)) {
    await selectPrediction(state.selectedPredictionId);
  } else if (activePreds.length) {
    await selectPrediction(activePreds[activePreds.length - 1].prediction_id);
  }
}

async function refreshAll() {
  const [readyRes, runsRes, evRes] = await Promise.all([
    fetchJson("/api/v1/health/ready"),
    fetchJson("/api/v1/runs?limit=20"),
    fetchJson("/api/v1/evidence"),
  ]);

  if (readyRes.payload) state.readiness = readyRes.payload;
  if (evRes.ok) state.evidence = evRes.payload;

  if (runsRes.ok && runsRes.payload?.runs) {
    state.runs = runsRes.payload.runs;
    if (!state.runs.some((r) => r.run_id === state.selectedRunId)) {
      state.selectedRunId = state.runs[0]?.run_id || null;
    }
    state.lastError = null;
  } else if (!runsRes.ok) {
    state.lastError =
      runsRes.payload?.detail?.message || "Could not reach run history endpoint.";
  }

  await loadRunScopedData();
}

function stopPlayback() {
  state.playing = false;
  if (state.playTimer !== null) {
    window.clearInterval(state.playTimer);
    state.playTimer = null;
  }
  renderRunAndTickerControls();
}

function startPlayback() {
  if (state.barsChrono.length <= 1) return;
  if (state.scrubIndex >= state.barsChrono.length - 1) {
    state.scrubIndex = 0;
  }
  state.playing = true;
  const intervalMs = Number(dom.speedSelect.value) || 1200;
  state.playTimer = window.setInterval(() => {
    if (state.scrubIndex >= state.barsChrono.length - 1) {
      stopPlayback();
      return;
    }
    state.scrubIndex += 1;
    renderRunAndTickerControls();
    renderOverview();
    renderPredictionsArea();
  }, intervalMs);
  renderRunAndTickerControls();
}

dom.refreshBtn.addEventListener("click", () => refreshAll());
dom.retryBtn.addEventListener("click", () => refreshAll());

dom.runSelect.addEventListener("change", () => {
  state.selectedRunId = dom.runSelect.value || null;
  stopPlayback();
  state.selectedPredictionId = null;
  loadRunScopedData();
});

dom.tickerSelect.addEventListener("change", () => {
  state.selectedTicker = dom.tickerSelect.value || null;
  stopPlayback();
  state.selectedPredictionId = null;
  loadRunScopedData();
});

dom.timelineSlider.addEventListener("input", () => {
  state.scrubIndex = Number(dom.timelineSlider.value) || 0;
  stopPlayback();
  renderRunAndTickerControls();
  renderOverview();
  renderPredictionsArea();
});

dom.stepBackBtn.addEventListener("click", () => {
  stopPlayback();
  state.scrubIndex = Math.max(0, state.scrubIndex - 1);
  renderRunAndTickerControls();
  renderOverview();
  renderPredictionsArea();
});

dom.stepForwardBtn.addEventListener("click", () => {
  stopPlayback();
  state.scrubIndex = Math.min(Math.max(0, state.barsChrono.length - 1), state.scrubIndex + 1);
  renderRunAndTickerControls();
  renderOverview();
  renderPredictionsArea();
});

dom.playPauseBtn.addEventListener("click", () => {
  if (state.playing) {
    stopPlayback();
  } else {
    startPlayback();
  }
});

dom.speedSelect.addEventListener("change", () => {
  if (state.playing) {
    stopPlayback();
    startPlayback();
  }
});

dom.statePreviewSelect.addEventListener("change", () => {
  stopPlayback();
  state.previewMode = dom.statePreviewSelect.value;
  renderAll();
});

dom.pollToggle.addEventListener("change", () => {
  if (state.pollTimer !== null) {
    window.clearInterval(state.pollTimer);
    state.pollTimer = null;
  }
  if (dom.pollToggle.checked) {
    state.pollTimer = window.setInterval(() => {
      if (!document.hidden && state.previewMode === "live") {
        refreshAll();
      }
    }, 10000);
  }
});

dom.toggleChartTableBtn.addEventListener("click", () => {
  const expanded = dom.toggleChartTableBtn.getAttribute("aria-expanded") === "true";
  dom.toggleChartTableBtn.setAttribute("aria-expanded", expanded ? "false" : "true");
  dom.toggleChartTableBtn.textContent = expanded ? "Show Chart Data Table" : "Hide Chart Data Table";
  dom.chartDataTableWrap.hidden = expanded;
});

refreshAll();
