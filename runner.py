import json
import os
import time
import traceback
from typing import Any, Dict
import warnings

# Keep runner logs clean (we can address these upstream later).
warnings.filterwarnings('ignore', category=FutureWarning)


from av_client import AlphaVantageClient
from engine import scan_watchlist_quad
from auto_exec import reconcile_and_execute, try_send_entries, AutoExecConfig

CFG_PATH = os.getenv("ZTOCKLY_RUNTIME_CONFIG_PATH", "/state/runtime_config.json")
LP_CACHE_PATH = os.getenv("ZTOCKLY_LP_CACHE_PATH", "/state/last_price_cache.json")
DEFAULT_INTERVAL = float(os.getenv("RUNNER_INTERVAL_SECONDS", "45"))

def _lp_key(sym: str) -> str:
    return str(sym).upper().strip()

def _load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}

def _atomic_write_json(path: str, data: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except Exception:
        pass

def _load_lp_cache() -> Dict[str, float]:
    d = _load_json(LP_CACHE_PATH)
    out: Dict[str, float] = {}
    for k, v in (d or {}).items():
        try:
            out[str(k)] = float(v)
        except Exception:
            pass
    return out

def _save_lp_cache(cache: Dict[str, float]) -> None:
    _atomic_write_json(LP_CACHE_PATH, cache)

def _build_autoexec_cfg(cfg: Dict[str, Any]) -> AutoExecConfig | None:
    ae = cfg.get("autoexec_cfg")
    if not isinstance(ae, dict):
        return None
    if not ae.get("enabled", False):
        return None

    # Ensure engines is Tuple[str, ...] as required by AutoExecConfig
    if isinstance(ae.get("engines"), list):
        ae["engines"] = tuple(ae["engines"])
    elif isinstance(ae.get("engines"), str):
        ae["engines"] = (ae["engines"],)

    return AutoExecConfig(**ae)

def main():
    print(f"[runner] cfg={CFG_PATH} lp_cache={LP_CACHE_PATH} default_interval={DEFAULT_INTERVAL}s")
    while True:
        t0 = time.time()
        try:
            cfg = _load_json(CFG_PATH)

            symbols = [s for s in (cfg.get("symbols") or []) if isinstance(s, str) and s.strip()]
            if not symbols:
                print("[runner] no symbols; sleeping 2s")
                time.sleep(2.0)
                continue

            interval = str(cfg.get("interval") or "1min")
            mode = str(cfg.get("mode") or "Cleaner signals")
            pro_mode = bool(cfg.get("pro_mode") or False)

            allow_opening = bool(cfg.get("allow_opening", True))
            allow_midday = bool(cfg.get("allow_midday", False))
            allow_power = bool(cfg.get("allow_power", True))
            allow_premarket = bool(cfg.get("allow_premarket", False))
            allow_afterhours = bool(cfg.get("allow_afterhours", False))

            client = AlphaVantageClient()

            lp_cache = _load_lp_cache()
            def fetch_last_cache_only(sym: str):
                return lp_cache.get(_lp_key(sym))

            aecfg = _build_autoexec_cfg(cfg)

            # Pre-scan: reconcile + try_send_entries
            if aecfg is not None:
                reconcile_and_execute(
                    aecfg,
                    allow_premarket,
                    allow_opening,
                    allow_midday,
                    allow_power,
                    allow_afterhours,
                    fetch_last_cache_only,
                )
                try_send_entries(aecfg, allow_opening, allow_midday, allow_power, fetch_last_cache_only)

            # Scan pass
            rev, ride, swing, mss = scan_watchlist_quad(
                client,
                symbols,
                interval=interval,
                mode=mode,
                pro_mode=pro_mode,
                allow_opening=allow_opening,
                allow_midday=allow_midday,
                allow_power=allow_power,
                allow_premarket=allow_premarket,
                allow_afterhours=allow_afterhours,
                use_last_closed_only=bool(cfg.get("use_last_closed_only", False)),
                bar_closed_guard=bool(cfg.get("bar_closed_guard", True)),
                vwap_logic=str(cfg.get("vwap_logic") or "session"),
                session_vwap_include_premarket=bool(cfg.get("session_vwap_include_premarket", False)),
                fib_lookback_bars=int(cfg.get("fib_lookback_bars") or 120),
                enable_htf_bias=bool(cfg.get("enable_htf_bias", False)),
                htf_interval=str(cfg.get("htf_interval") or "15min"),
                htf_strict=bool(cfg.get("htf_strict", False)),
                killzone_preset=str(cfg.get("killzone_preset") or "Custom (use toggles)"),
                liquidity_weighting=float(cfg.get("liquidity_weighting") or 0.55),
                orb_minutes=int(cfg.get("orb_minutes") or 15),
                entry_model=str(cfg.get("entry_model") or "VWAP reclaim limit"),
                slippage_mode=str(cfg.get("slippage_mode") or "Fixed cents"),
                fixed_slippage_cents=float(cfg.get("fixed_slippage_cents") or 0.02),
                atr_fraction_slippage=float(cfg.get("atr_fraction_slippage") or 0.15),
                target_atr_pct=cfg.get("target_atr_pct"),
            )

            # Update disk last_price_cache from results
            all_results = list(rev or []) + list(ride or []) + list(swing or []) + list(mss or [])
            updated = 0
            for r in all_results:
                sym = getattr(r, "symbol", None)
                last = getattr(r, "last_price", None)
                if last is None:
                    last = getattr(r, "last", None)
                if sym and last is not None:
                    try:
                        v = float(str(last).strip())
                        if v == v and v not in (float("inf"), float("-inf")):
                            lp_cache[_lp_key(sym)] = v
                            updated += 1
                    except Exception:
                        pass
            _save_lp_cache(lp_cache)

            # Post-scan: immediate-on-stage entry placement
            if aecfg is not None:
                try_send_entries(aecfg, allow_opening, allow_midday, allow_power, fetch_last_cache_only)

            dt = time.time() - t0
            interval_sec = float(cfg.get("runner_interval_seconds") or DEFAULT_INTERVAL)
            sleep_for = max(1.0, interval_sec - dt)
            print(f"[runner] ok syms={len(symbols)} scan_dt={dt:.2f}s lp_updates={updated} sleep={sleep_for:.1f}s")
            time.sleep(sleep_for)

        except Exception as e:
            print(f"[runner] error: {e}")
            traceback.print_exc()
            time.sleep(5.0)

if __name__ == "__main__":
    main()
