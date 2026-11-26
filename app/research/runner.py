"""Experiment runner (v1.3 §10, roadmap Phase 6).

run_experiment():
  1. records the experiment as RUNNING (kept as FAILED with the traceback if anything goes wrong),
  2. counts its variants in the strategy-set trial counter *before* running,
  3. splits sessions into development / sealed hold-out, builds walk-forward folds on development only,
  4. simulates every variant over the development sessions with the production pipeline + risk engine,
  5. per fold selects the best variant on the train window (selection metric, minimum trades) and evaluates
     it on the next test window; the concatenated test windows are the walk-forward OOS aggregate,
  6. reports metrics, segmentation (DTE / strategy / regime / time of day), session-block bootstrap CIs,
     Deflated Sharpe with the global trial count, and the §10.4 promotion screen,
  7. optionally (explicit actor + purpose) evaluates the spec's frozen `holdout_variant` on the hold-out once:
     the access is logged and the hold-out retired.
A rerun of the same spec on unchanged code and data must give the same `results_hash`.
"""
from __future__ import annotations

import json
import traceback
from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.clock import iso, now_ist
from app.database.models import ExperimentResults, Experiments
from app.research import metrics as M
from app.research.registry import code_fingerprint, current_holdout, register_trials, request_holdout
from app.research.simulate import HistoryContext, simulate
from app.research.spec import ExperimentSpec, build_strategies, sha, variant_id
from app.research.splits import walk_forward


def _frame(trades: list[dict]) -> pd.DataFrame:
    cols = ["trade_id", "session", "strategy", "dte", "net_pnl", "gross_pnl", "charges", "risk_inr", "entry_px",
            "exit_px", "qty", "exit_reason", "entry_ns", "exit_ns", "low_fidelity"]
    return pd.DataFrame(trades, columns=cols) if trades else pd.DataFrame(columns=cols)


def _select(t: pd.DataFrame, sessions) -> pd.DataFrame:
    return t[t["session"].isin(set(sessions))]


def _score(m: dict, metric: str, min_trades: int):
    v = m.get(metric)
    return None if m["trades"] < min_trades or v is None else v


def run_experiment(spec: ExperimentSpec, research: Engine, ctx: HistoryContext, actor: str,
                   evaluate_holdout: bool = False, holdout_purpose: str | None = None,
                   out_root: Path | None = None, rerun_of: str | None = None, bootstrap_n: int = 2000) -> dict:
    exp_id = f"exp_{now_ist().strftime('%Y%m%d-%H%M%S-%f')}_{spec.name}"
    strategy_set = "+".join(sorted(spec.strategies))
    variants = spec.variants()
    with Session(research) as s:
        s.add(Experiments(id=exp_id, name=spec.name, hypothesis=spec.hypothesis,
                          spec=json.dumps(spec.to_dict(), sort_keys=True, default=str), spec_hash=spec.spec_hash,
                          strategy_set=strategy_set, code_fingerprint=json.dumps(code_fingerprint()),
                          status="RUNNING", actor=actor, started_at=iso(), variants_tested=len(variants),
                          rerun_of=rerun_of))
        s.commit()
    try:
        result = _run(exp_id, spec, research, ctx, actor, variants, strategy_set, evaluate_holdout, holdout_purpose,
                      out_root, rerun_of, bootstrap_n)
    except BaseException as e:
        with Session(research) as s:
            row = s.get(Experiments, exp_id)
            row.status, row.finished_at = "FAILED", iso()
            row.error = "".join(traceback.format_exception(type(e), e, e.__traceback__))[-8000:]
            s.commit()
        raise
    return result


def _run(exp_id, spec, research, ctx, actor, variants, strategy_set, evaluate_holdout, holdout_purpose, out_root,
         rerun_of, bootstrap_n) -> dict:
    sessions = [d for d in ctx.days if (not spec.session_from or d >= spec.session_from)
                and (not spec.session_to or d <= spec.session_to)]
    holdout = current_holdout(research, ctx.days)
    hold_sessions = json.loads(holdout.sessions) if holdout else []
    dev = [d for d in sessions if d not in set(hold_sessions)]
    folds = walk_forward(dev, spec.walk_forward)
    if not folds:
        raise ValueError(f"not enough development sessions ({len(dev)}) for the walk-forward layout")
    n_trials = register_trials(research, strategy_set, variants, exp_id)
    data_fp = ctx.data_fingerprint()
    from app.risk.config import RiskConfig
    rc = RiskConfig.load()
    with Session(research) as s:
        row = s.get(Experiments, exp_id)
        row.data_fingerprint, row.trials_total = json.dumps(data_fp), n_trials
        row.risk_config, row.fee_version, row.fill_model = f"{rc.version}:{rc.sha256[:12]}", ctx.fees.version, ctx.fill.version
        row.holdout_id = holdout.id if holdout else None
        s.commit()

    # ---- simulate every variant over development sessions (one continuous run per variant)
    runs = simulate(ctx, {vid: build_strategies(spec.strategies, p, spec.history_overrides)
                          for vid, p in variants.items()}, days=dev)
    regimes = M.session_regimes(ctx.index)
    frames = {vid: _frame(r.trades) for vid, r in runs.items()}
    rows: list[tuple[str, str, dict, dict]] = []          # (variant, scope, params, metrics)
    trial_sr = []
    for vid, t in frames.items():
        m = M.core(t, dev) | {"audit_hash": runs[vid].audit_hash, "audit_ok": runs[vid].audit_ok,
                              "signals": len(runs[vid].signals)}
        rows.append((vid, "full_dev", variants[vid], m))
        d = M.daily_pnl(t, dev)
        if d.std(ddof=1) > 0:
            trial_sr.append(float(d.mean() / d.std(ddof=1)))
        for f in folds:
            rows.append((vid, f"fold:{f.index}:train", variants[vid], M.core(_select(t, f.train), list(f.train))))
            rows.append((vid, f"fold:{f.index}:test", variants[vid], M.core(_select(t, f.test), list(f.test))))
    by = {(v, sc): m for v, sc, _, m in rows}

    # ---- walk-forward selection and OOS aggregate
    wf, oos_parts, oos_sessions = [], [], []
    for f in folds:
        scored = [(_score(by[(v, f"fold:{f.index}:train")], spec.selection_metric, spec.walk_forward.min_train_trades), v)
                  for v in variants]
        eligible = [(sc, v) for sc, v in scored if sc is not None]
        chosen = max(eligible, key=lambda x: (x[0], x[1]))[1] if eligible else None
        wf.append({"fold": f.index, "train": [f.train[0], f.train[-1]], "test": [f.test[0], f.test[-1]],
                   "selected": chosen, "train_score": dict((v, sc) for sc, v in scored),
                   "test_metrics": by[(chosen, f"fold:{f.index}:test")] if chosen else None})
        oos_sessions += list(f.test)
        if chosen:
            oos_parts.append(_select(frames[chosen], f.test))
    oos_t = pd.concat(oos_parts) if oos_parts else _frame([])
    oos = M.core(oos_t, oos_sessions)
    oos_seg = M.segmented(oos_t, oos_sessions, regimes)
    seed = int(sha(spec.to_dict())[:8], 16)
    boot = M.session_bootstrap(oos_t, oos_sessions, n=bootstrap_n, seed=seed)
    dsr = M.deflated_sharpe(M.daily_pnl(oos_t, oos_sessions), trial_sr, n_trials)
    observed = oos_t[~oos_t["low_fidelity"].astype(bool)] if len(oos_t) else oos_t
    integrity = {"quality_failed_days": [d for d in data_fp["quality_failed_days"] if d in set(oos_sessions)]}
    screen = M.promotion_screen(oos, oos_seg, boot, dsr, integrity, len(observed),
                                int(observed["session"].nunique()) if len(observed) else 0, n_trials, False)
    rows.append(("walk_forward", "wf_oos", {"selection": [w["selected"] for w in wf]},
                 oos | {"segments": oos_seg, "bootstrap": boot, "dsr": dsr}))

    # ---- hold-out (explicit, once)
    hold = None
    if evaluate_holdout:
        if not spec.holdout_variant:
            raise ValueError("evaluate_holdout needs spec.holdout_variant (one frozen parameter set)")
        h = request_holdout(research, holdout.id if holdout else None, exp_id, actor, holdout_purpose or "")
        hs = json.loads(h.sessions)
        hv = variant_id(spec.holdout_variant)
        register_trials(research, strategy_set, {hv: spec.holdout_variant}, exp_id)
        all_days = sorted(set(dev) | set(hs))
        hr = simulate(ctx, {hv: build_strategies(spec.strategies, spec.holdout_variant, spec.history_overrides)},
                      days=all_days)[hv]
        ht = _select(_frame(hr.trades), hs)
        hold = M.core(ht, hs) | {"segments": M.segmented(ht, hs, regimes),
                                 "bootstrap": M.session_bootstrap(ht, hs, n=bootstrap_n, seed=seed),
                                 "holdout_id": h.id, "sessions_range": [hs[0], hs[-1]]}
        rows.append((hv, "holdout", spec.holdout_variant, hold))

    results = {"variants": {v: by[(v, "full_dev")] for v in variants}, "walk_forward": wf, "oos": oos,
               "oos_segments": oos_seg, "bootstrap": boot, "dsr": dsr, "screen": screen, "holdout": hold}
    results_hash = sha(results)

    # ---- persist + report
    out = (out_root or ctx.cfg.path("derived").parent / "experiments") / exp_id
    out.mkdir(parents=True, exist_ok=True)
    report = {"experiment_id": exp_id, "name": spec.name, "hypothesis": spec.hypothesis, "spec": spec.to_dict(),
              "spec_hash": spec.spec_hash, "label": ctx.label, "data": data_fp, "code": code_fingerprint(),
              "risk_config": f"{rc.version}:{rc.sha256[:12]}", "fee_version": ctx.fees.version,
              "fill_model": ctx.fill.__dict__, "sessions": {"development": [dev[0], dev[-1], len(dev)],
                                                            "holdout": [hold_sessions[0], hold_sessions[-1],
                                                                        len(hold_sessions)] if hold_sessions else None},
              "variants": variants, "trials_total_for_strategy_set": n_trials, "results": results,
              "results_hash": results_hash, "rerun_of": rerun_of}
    (out / "report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    (out / "report.md").write_text(markdown(report), encoding="utf-8")
    reproduced = None
    with Session(research) as s:
        if rerun_of:
            prev = s.get(Experiments, rerun_of)
            reproduced = int(prev is not None and prev.results_hash == results_hash)
        for v, sc, p, m in rows:
            s.add(ExperimentResults(experiment_id=exp_id, variant_id=v, scope=sc, params=json.dumps(p, sort_keys=True),
                                    metrics=json.dumps(m, sort_keys=True, default=str)))
        row = s.get(Experiments, exp_id)
        row.status, row.finished_at, row.results_hash = "COMPLETED", iso(), results_hash
        row.verdict, row.report_path, row.reproduced = screen["verdict"], str(out), reproduced
        row.holdout_evaluated = int(hold is not None)
        s.commit()
    return report | {"reproduced": reproduced, "report_dir": str(out)}


# ---------------------------------------------------------------------------------------------- report
def _fmt(v):
    return "–" if v is None else f"{v:,.2f}" if isinstance(v, float) else str(v)


def markdown(r: dict) -> str:
    res = r["results"]
    L = [f"# Experiment {r['experiment_id']}", "", f"**Hypothesis:** {r['hypothesis']}", "",
         f"Data: {r['label']} · development {r['sessions']['development']} · hold-out {r['sessions']['holdout']}",
         f"Spec hash `{r['spec_hash'][:12]}` · results hash `{r['results_hash'][:12]}` · code "
         f"`{r['code']['source_sha256'][:12]}` (git {str(r['code'].get('head'))[:8]}{' dirty' if r['code'].get('dirty') else ''})",
         f"Variants in this experiment: {len(r['variants'])} · variants ever tested for this strategy set: "
         f"{r['trials_total_for_strategy_set']}", "", "## Variants (full development period)", "",
         "| variant | params | trades | net ₹ | exp R | PF | win % | max DD R | sharpe |", "|---|---|---|---|---|---|---|---|---|"]
    for v, m in res["variants"].items():
        p = ", ".join(f"{k}={x}" for k, x in r["variants"][v].items()) or "(defaults)"
        L.append(f"| {v} | {p} | {m['trades']} | {_fmt(m['net_pnl'])} | {_fmt(m['net_expectancy_r'])} | "
                 f"{_fmt(m['profit_factor'])} | {_fmt(m['win_rate_pct'])} | {_fmt(m['max_drawdown_r'])} | "
                 f"{_fmt(m['sharpe_session'])} |")
    L += ["", "## Walk-forward", "", "| fold | train | test | selected | OOS trades | OOS net ₹ |", "|---|---|---|---|---|---|"]
    for w in res["walk_forward"]:
        tm = w["test_metrics"] or {}
        L.append(f"| {w['fold']} | {w['train'][0]}..{w['train'][1]} | {w['test'][0]}..{w['test'][1]} | "
                 f"{w['selected'] or 'none eligible'} | {tm.get('trades', 0)} | {_fmt(tm.get('net_pnl'))} |")
    o, b, d = res["oos"], res["bootstrap"], res["dsr"]
    L += ["", "## Walk-forward OOS aggregate", "",
          f"trades {o['trades']} over {o['sessions']} sessions · net ₹{_fmt(o['net_pnl'])} · expectancy "
          f"{_fmt(o['net_expectancy_r'])} R · PF {_fmt(o['profit_factor'])} · max DD {_fmt(o['max_drawdown_r'])} R",
          f"bootstrap 95% CI net expectancy ₹ {b.get('net_expectancy_inr')} · DSR {d.get('dsr', d.get('reason'))} "
          f"(trials {d.get('n_trials')})", "", "| DTE | trades | net ₹ | exp R |", "|---|---|---|---|"]
    for k, m in res["oos_segments"].get("by_dte", {}).items():
        L.append(f"| {k} | {m['trades']} | {_fmt(m['net_pnl'])} | {_fmt(m['net_expectancy_r'])} |")
    L += ["", "## Promotion screen (§10.4)", "", f"**Verdict: {res['screen']['verdict']}**", "", "| gate | status |", "|---|---|"]
    for k, g in res["screen"]["gates"].items():
        L.append(f"| {k} | {g['status']} |")
    if res.get("holdout"):
        h = res["holdout"]
        L += ["", "## Hold-out (single inspection — now retired)", "",
              f"trades {h['trades']} · net ₹{_fmt(h['net_pnl'])} · expectancy {_fmt(h['net_expectancy_r'])} R · PF "
              f"{_fmt(h['profit_factor'])}"]
    L += ["", "_Screening thresholds are governance gates, not evidence of future profitability._", ""]
    return "\n".join(L)
