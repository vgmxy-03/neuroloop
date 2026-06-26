"""
Mind Monitor → OSC receiver → WebSocket broadcast → browser dashboard.

Mind Monitor OSC settings:
  Host: 192.168.1.9   Port: 5000
"""

import asyncio
import json
import math
import threading
import time
import collections
from pythonosc import dispatcher, osc_server
import websockets

# ── shared state ──────────────────────────────────────────────────────────────
state = {
    "eeg":       [0.0, 0.0, 0.0, 0.0],   # latest sample (TP9, AF7, AF8, TP10)
    "eeg_buf":   [],                        # all raw samples since last broadcast
    "alpha":     [0.0, 0.0, 0.0, 0.0],
    "beta":      [0.0, 0.0, 0.0, 0.0],
    "theta":     [0.0, 0.0, 0.0, 0.0],
    "delta":     [0.0, 0.0, 0.0, 0.0],
    "gamma":     [0.0, 0.0, 0.0, 0.0],
    "horseshoe": [4.0, 4.0, 4.0, 4.0],   # 1=good, 2=ok, 4=bad
    "batt":      -1,
    "bpm":        0,
    "optics":     [0.0, 0.0, 0.0],          # raw PPG optical channels (latest)
    "optics_buf": [],                         # buffered PPG samples since last broadcast
    "acc":        [0.0, 0.0, 0.0],           # accelerometer x/y/z (g)
    "gyro":       [0.0, 0.0, 0.0],           # gyroscope x/y/z (deg/s)
    "touching":   0,                          # 1 if recently received touching=1, else 0
    "blink":      0,                          # monotonically incrementing event count
    "jaw_clench": 0,                          # monotonically incrementing event count
    "sr":        256,                       # sample rate hint
    "ts":        0,
    "stability": {
        "all_electrodes_ok": False,
        "alpha_cv_ok":       False,
        "not_railing":       True,
        "clean_secs":        0.0,
        "required_secs":     15.0,
        "alpha_cv_af7":      None,
        "alpha_cv_af8":      None,
        "cv_thresh":         0.25,
        "ready":             False,
    },
}
state_lock = threading.Lock()
clients: set = set()

OSC_PORT = 5000
WS_PORT  = 8765

STABLE_SECS     = 15
ALPHA_CV_THRESH = 0.25
WINDOW_FPS      = 25
WINDOW          = int(10 * WINDOW_FPS)
RAIL_THRESH     = 1000.0   # µV — Muse can legitimately reach ±500 µV with poor contact

_alpha_bufs  = [collections.deque(maxlen=WINDOW) for _ in range(4)]
_eeg_bufs    = [collections.deque(maxlen=WINDOW) for _ in range(4)]
_clean_since = None

BLINK_GRACE      = 0.6    # seconds to ignore railing after a blink event
TOUCH_TIMEOUT    = 1.5    # touching_forehead resets to 0 if no packet received in this many s
BLINK_DEBOUNCE   = 0.5    # minimum seconds between counted blink events
JAW_DEBOUNCE     = 0.8    # minimum seconds between counted jaw-clench events
_blink_grace_until = 0.0
_touching_last_ts  = 0.0
_blink_last_ts     = 0.0
_jaw_last_ts       = 0.0

# ── PPG / BPM estimation from /muse/optics ────────────────────────────────────
# Use channel index 1 (IR) — strongest pulsatile signal on Muse 2
_ppg_buf    = collections.deque(maxlen=256 * 4)   # ~4 s at 256 Hz
_ibi_buf    = collections.deque(maxlen=10)          # last 10 inter-beat intervals (ms)
_ppg_last_peak   = None    # timestamp of last detected peak
_ppg_last_val    = 0.0
_ppg_rising      = False
_ppg_thresh_high = None    # adaptive threshold
_ppg_thresh_low  = None

def _update_ppg(args):
    """Peak detection on PPG IR channel → BPM via 10-element IBI average (Pope/OpenBCI method)."""
    global _ppg_last_peak, _ppg_last_val, _ppg_rising, _ppg_thresh_high, _ppg_thresh_low

    if not args:
        return
    # use channel 1 (index 1 = IR on Muse 2); fall back to 0
    val = float(args[1] if len(args) > 1 else args[0])
    now = time.time()
    _ppg_buf.append(val)

    # initialise adaptive thresholds
    if _ppg_thresh_high is None:
        _ppg_thresh_high = val
        _ppg_thresh_low  = val
        return

    # track running min/max for adaptive threshold
    _ppg_thresh_high = max(_ppg_thresh_high * 0.995, val)
    _ppg_thresh_low  = min(_ppg_thresh_low  * 1.005, val)
    amplitude = _ppg_thresh_high - _ppg_thresh_low
    if amplitude < 0.001:
        return
    thresh = _ppg_thresh_low + amplitude * 0.6   # 60% of amplitude above baseline

    # rising-edge peak detector
    was_below = _ppg_last_val < thresh
    now_above = val >= thresh

    if was_below and now_above and not _ppg_rising:
        _ppg_rising = True
        if _ppg_last_peak is not None:
            ibi_ms = (now - _ppg_last_peak) * 1000
            if 333 < ibi_ms < 2000:   # 30–180 BPM range
                _ibi_buf.append(ibi_ms)
                if len(_ibi_buf) >= 3:
                    avg_ibi = sum(_ibi_buf) / len(_ibi_buf)
                    with state_lock:
                        state["bpm"] = round(60000 / avg_ibi, 1)
        _ppg_last_peak = now

    if val < thresh:
        _ppg_rising = False

    _ppg_last_val = val

# sample rate estimator
_sr_times = collections.deque(maxlen=256)

_seen_paths: set = set()


# ── helpers ───────────────────────────────────────────────────────────────────
def _cv(buf):
    vals = list(buf)
    if not vals:
        return None
    m = sum(vals) / len(vals)
    if m == 0:
        return None
    std = math.sqrt(sum((x - m) ** 2 for x in vals) / len(vals))
    return std / m


def _update_stability(hs, eeg, alpha):
    global _clean_since
    now = time.time()

    for i in range(4):
        _alpha_bufs[i].append(alpha[i] if alpha[i] else 0.0)
        _eeg_bufs[i].append(abs(eeg[i]) if eeg[i] else 0.0)

    all_ok  = all(hs[i] <= 2 for i in range(4))
    railing = any(any(v > RAIL_THRESH for v in _eeg_bufs[i]) for i in range(4))

    # suppress railing during blink grace window — eye blinks cause short EEG spikes
    # that should not reset the stability timer
    in_blink_grace = now < _blink_grace_until
    effective_railing = railing and not in_blink_grace

    if all_ok and not effective_railing:
        if _clean_since is None:
            _clean_since = now
    else:
        _clean_since = None

    clean_secs = (now - _clean_since) if _clean_since else 0.0

    cv7 = _cv(_alpha_bufs[1]) if len(_alpha_bufs[1]) >= WINDOW // 2 else None
    cv8 = _cv(_alpha_bufs[2]) if len(_alpha_bufs[2]) >= WINDOW // 2 else None
    cv_ok = (cv7 is not None and cv7 < ALPHA_CV_THRESH and
             cv8 is not None and cv8 < ALPHA_CV_THRESH)

    state["stability"] = {
        "all_electrodes_ok": all_ok,
        "alpha_cv_ok":       cv_ok,
        "not_railing":       not effective_railing,
        "blink_suppressed":  in_blink_grace and railing,
        "clean_secs":        round(clean_secs, 1),
        "required_secs":     STABLE_SECS,
        "alpha_cv_af7":      round(cv7, 3) if cv7 is not None else None,
        "alpha_cv_af8":      round(cv8, 3) if cv8 is not None else None,
        "cv_thresh":         ALPHA_CV_THRESH,
        "ready":             clean_secs >= STABLE_SECS and cv_ok and not railing,
    }


# ── OSC handlers ──────────────────────────────────────────────────────────────
def _update(key, args):
    with state_lock:
        state[key] = list(args)
        if key == "eeg":
            state["eeg_buf"].append(list(args))
            # estimate sample rate
            now = time.time()
            _sr_times.append(now)
            if len(_sr_times) >= 10:
                elapsed = _sr_times[-1] - _sr_times[0]
                if elapsed > 0:
                    state["sr"] = round((len(_sr_times) - 1) / elapsed)
        state["ts"] = time.time()
        if key in ("horseshoe", "eeg", "alpha"):
            _update_stability(
                state["horseshoe"],
                state["eeg"],
                state["alpha"],
            )


def _update_scalar(key, args, scale=1.0):
    with state_lock:
        state[key] = (args[0] / scale) if args else 0
        state["ts"] = time.time()


def _debug_handler(addr, *args):
    if addr not in _seen_paths:
        _seen_paths.add(addr)
        print(f"[OSC new path] {addr}  args={args[:3]}")


def make_handler(key):
    return lambda addr, *args: _update(key, args)


def make_scalar_handler(key, scale=1.0):
    return lambda addr, *args: _update_scalar(key, args, scale)


def build_dispatcher():
    d = dispatcher.Dispatcher()
    for band in ("alpha", "beta", "theta", "delta", "gamma"):
        d.map(f"/muse/elements/{band}_absolute", make_handler(band))
    d.map("/muse/eeg",                         make_handler("eeg"))
    d.map("/muse/elements/horseshoe",          make_handler("horseshoe"))
    d.map("/muse/batt",                        make_scalar_handler("batt", scale=100.0))
    for path in ("/muse/bpm",
                 "/muse/elements/bpm",
                 "/muse/elements/experimental/bpm",
                 "/muse/ppg/bpm"):
        d.map(path, make_scalar_handler("bpm"))

    # raw PPG → store optics + buffer for browser-side FFT BPM
    def optics_handler(addr, *args):
        with state_lock:
            state["optics"] = list(args)
            state["optics_buf"].append(list(args))
            state["ts"] = time.time()
        _update_ppg(args)
    d.map("/muse/optics", optics_handler)

    d.map("/muse/acc",  make_handler("acc"))
    d.map("/muse/gyro", make_handler("gyro"))
    def touching_handler(addr, *args):
        global _touching_last_ts
        val = int(args[0]) if args else 0
        with state_lock:
            state["touching"] = val
            if val:
                _touching_last_ts = time.time()
    d.map("/muse/elements/touching_forehead", touching_handler)

    def blink_handler(addr, *args):
        global _blink_grace_until, _blink_last_ts
        now = time.time()
        _blink_grace_until = now + BLINK_GRACE
        if now - _blink_last_ts < BLINK_DEBOUNCE:
            return   # duplicate OSC packet within debounce window — ignore
        _blink_last_ts = now
        with state_lock:
            state["blink"] += 1
    d.map("/muse/elements/blink", blink_handler)

    def jaw_handler(addr, *args):
        global _jaw_last_ts
        now = time.time()
        if now - _jaw_last_ts < JAW_DEBOUNCE:
            return   # duplicate OSC packet — ignore
        _jaw_last_ts = now
        with state_lock:
            state["jaw_clench"] += 1
    d.map("/muse/elements/jaw_clench", jaw_handler)
    d.set_default_handler(_debug_handler)
    return d


def run_osc():
    server = osc_server.ThreadingOSCUDPServer(("0.0.0.0", OSC_PORT), build_dispatcher())
    print(f"[OSC]  listening on 0.0.0.0:{OSC_PORT}")
    server.serve_forever()


# ── WebSocket broadcast ───────────────────────────────────────────────────────
async def ws_handler(ws):
    clients.add(ws)
    print(f"[WS]   client connected  (total={len(clients)})")
    try:
        await ws.wait_closed()
    finally:
        clients.discard(ws)
        print(f"[WS]   client disconnected (total={len(clients)})")


async def broadcast_loop():
    while True:
        await asyncio.sleep(0.04)          # ~25 fps
        if not clients:
            continue
        with state_lock:
            # decay touching_forehead if no recent packet
            if state["touching"] and (time.time() - _touching_last_ts) > TOUCH_TIMEOUT:
                state["touching"] = 0
            payload = json.dumps(state)
            state["eeg_buf"]    = []
            state["optics_buf"] = []
        dead = set()
        for ws in list(clients):
            try:
                await ws.send(payload)
            except Exception:
                dead.add(ws)
        clients.difference_update(dead)


async def main():
    osc_thread = threading.Thread(target=run_osc, daemon=True)
    osc_thread.start()

    async with websockets.serve(ws_handler, "0.0.0.0", WS_PORT):
        print(f"[WS]   server on ws://localhost:{WS_PORT}")
        print(f"\n  Mind Monitor OSC → 192.168.1.9:{OSC_PORT}\n")
        await broadcast_loop()


if __name__ == "__main__":
    asyncio.run(main())
