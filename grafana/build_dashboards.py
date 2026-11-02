"""Dashboards as code: builds grafana/dashboards/*.json.

Run ``python grafana/build_dashboards.py`` after changing a panel. Grafana picks up the
JSON through file provisioning; nobody edits dashboards by hand in the UI.
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).parent / "dashboards"

CH = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse"}
PROM = {"type": "prometheus", "uid": "prometheus"}

# Amount in EUR: FX rates live in a ClickHouse dictionary, so conversion is a hash lookup.
FX = "dictGet('delta1.fx_dict', 'rate_to_eur', tuple(currency))"


def eur(expr: str) -> str:
    return f"toFloat64({expr}) * {FX}"


# ClickHouse plugin query formats.
TIMESERIES, TABLE = 0, 1


class Board:
    def __init__(self, uid: str, title: str, refresh: str = "10s", time_from: str = "now-30m"):
        self.uid, self.title, self.refresh, self.time_from = uid, title, refresh, time_from
        self.panels: list[dict] = []
        self._next_id = 1
        self._x = self._y = self._row_h = 0

    def _place(self, w: int, h: int) -> dict:
        if self._x + w > 24:
            self._x, self._y = 0, self._y + self._row_h
            self._row_h = 0
        pos = {"x": self._x, "y": self._y, "w": w, "h": h}
        self._x += w
        self._row_h = max(self._row_h, h)
        return pos

    def row(self, title: str) -> None:
        if self._x:
            self._x, self._y = 0, self._y + self._row_h
        self.panels.append(
            {
                "id": self._next_id,
                "type": "row",
                "title": title,
                "collapsed": False,
                "gridPos": {"x": 0, "y": self._y, "w": 24, "h": 1},
                "panels": [],
            }
        )
        self._next_id += 1
        self._y += 1
        self._row_h = 0

    def add(
        self,
        kind: str,
        title: str,
        targets: list[dict],
        w: int,
        h: int,
        *,
        unit: str | None = None,
        decimals: int | None = None,
        description: str = "",
        options: dict | None = None,
        custom: dict | None = None,
        overrides: list | None = None,
        thresholds: list | None = None,
        color_mode: str | None = None,
    ) -> None:
        defaults: dict = {"custom": custom or {}}
        if unit:
            defaults["unit"] = unit
        if decimals is not None:
            defaults["decimals"] = decimals
        if thresholds:
            defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
        default_mode = "thresholds" if kind == "stat" else "palette-classic"
        defaults["color"] = {"mode": color_mode or default_mode}
        if color_mode == "fixed":
            defaults["color"]["fixedColor"] = "#5794F2"
        panel = {
            "id": self._next_id,
            "type": kind,
            "title": title,
            "description": description,
            "datasource": targets[0]["datasource"],
            "gridPos": self._place(w, h),
            "targets": targets,
            "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
            "options": options or {},
        }
        self.panels.append(panel)
        self._next_id += 1

    def json(self) -> dict:
        return {
            "uid": self.uid,
            "title": self.title,
            "tags": ["delta1"],
            "timezone": "browser",
            "schemaVersion": 39,
            "version": 1,
            "editable": True,
            "graphTooltip": 1,
            "refresh": self.refresh,
            "time": {"from": self.time_from, "to": "now"},
            "panels": self.panels,
            "templating": {"list": []},
            "annotations": {"list": []},
            "links": [
                {
                    "title": "Delta1 dashboards",
                    "type": "dashboards",
                    "tags": ["delta1"],
                    "asDropdown": False,
                }
            ],
        }


def ch(sql: str, fmt: int = TABLE, ref: str = "A") -> dict:
    return {
        "datasource": CH,
        "editorType": "sql",
        "format": fmt,
        "queryType": "timeseries" if fmt == TIMESERIES else "table",
        "rawSql": " ".join(sql.split()),
        "refId": ref,
    }


def prom(expr: str, legend: str = "", ref: str = "A") -> dict:
    return {"datasource": PROM, "expr": expr, "legendFormat": legend, "refId": ref}


STAT = {
    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
    "colorMode": "value",
    "graphMode": "none",
    "textMode": "value",
    "justifyMode": "center",
}
LINE = {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 12, "showPoints": "never"}
STACKED = {**LINE, "stacking": {"mode": "normal", "group": "A"}, "fillOpacity": 35}
TS_OPTS = {
    "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
    "tooltip": {"mode": "multi", "sort": "desc"},
}
BLUE = [{"color": "#5794F2", "value": None}]
GREEN = [{"color": "#73BF69", "value": None}]
PURPLE = [{"color": "#B877D9", "value": None}]
ORANGE = [{"color": "#FF9830", "value": None}]


# --------------------------------------------------------------------------- business
def business() -> Board:
    b = Board("delta1-business", "Delta1 · Platform live overview")

    casino_ggr = f"sum({eur('turnover - payout')})"
    sport_ggr = f"sum({eur('stake - payout')})"

    b.row("Headline (selected time range)")
    b.add(
        "stat",
        "Casino GGR",
        [ch(f"SELECT {casino_ggr} AS ggr FROM delta1.casino_ggr_1m WHERE $__timeFilter(minute)")],
        4,
        4,
        unit="currencyEUR",
        decimals=0,
        thresholds=GREEN,
        options=STAT,
        description="Gross gaming revenue = bets - wins, converted to EUR.",
    )
    b.add(
        "stat",
        "Sportsbook GGR",
        [ch(f"SELECT {sport_ggr} AS ggr FROM delta1.sport_ggr_1m WHERE $__timeFilter(minute)")],
        4,
        4,
        unit="currencyEUR",
        decimals=0,
        thresholds=GREEN,
        options=STAT,
        description="Stake - payout of bets settled in the range.",
    )
    b.add(
        "stat",
        "Turnover",
        [
            ch(
                f"""SELECT
                (SELECT sum({eur("turnover")}) FROM delta1.casino_ggr_1m
                 WHERE $__timeFilter(minute)) +
                (SELECT sum({eur("stake")}) FROM delta1.sport_ggr_1m
                 WHERE $__timeFilter(minute)) AS turnover"""
            )
        ],
        4,
        4,
        unit="currencyEUR",
        decimals=0,
        thresholds=BLUE,
        options=STAT,
    )
    b.add(
        "stat",
        "Casino RTP (actual)",
        [
            ch(
                f"""SELECT sum({eur("payout")}) / sum({eur("turnover")}) AS rtp
                FROM delta1.casino_ggr_1m WHERE $__timeFilter(minute)"""
            )
        ],
        4,
        4,
        unit="percentunit",
        decimals=2,
        thresholds=PURPLE,
        options=STAT,
        description="Wins / bets across all games. Theoretical RTPs are 95-99%.",
    )
    b.add(
        "stat",
        "Active casino players",
        [
            ch(
                "SELECT uniqMerge(players) AS players FROM delta1.casino_ggr_1m "
                "WHERE $__timeFilter(minute)"
            )
        ],
        4,
        4,
        thresholds=BLUE,
        options=STAT,
    )
    b.add(
        "stat",
        "Deposits",
        [
            ch(
                f"""SELECT sum({eur("amount")}) AS deposits
                FROM delta1.deposits WHERE $__timeFilter(ts)"""
            )
        ],
        4,
        4,
        unit="currencyEUR",
        decimals=0,
        thresholds=ORANGE,
        options=STAT,
    )

    b.row("Revenue in real time")
    b.add(
        "timeseries",
        "GGR per minute by brand (casino + sportsbook)",
        [
            ch(
                f"""SELECT minute AS time, brand, sum(ggr) AS ggr FROM (
                    SELECT minute, brand, {eur("turnover - payout")} AS ggr
                    FROM delta1.casino_ggr_1m WHERE $__timeFilter(minute)
                    UNION ALL
                    SELECT minute, brand, {eur("stake - payout")} AS ggr
                    FROM delta1.sport_ggr_1m WHERE $__timeFilter(minute)
                ) GROUP BY time, brand ORDER BY time""",
                TIMESERIES,
            )
        ],
        12,
        8,
        unit="currencyEUR",
        custom=STACKED,
        options=TS_OPTS,
        description="Casino GGR is volatile minute to minute: a single big win can push it "
        "negative. Sportsbook GGR is booked when bets settle.",
    )
    b.add(
        "timeseries",
        "Turnover per minute: casino vs sportsbook",
        [
            ch(
                f"""SELECT time, vertical, turnover_eur FROM (
                    SELECT minute AS time, 'casino' AS vertical,
                        sum({eur("turnover")}) AS turnover_eur
                    FROM delta1.casino_ggr_1m WHERE $__timeFilter(minute) GROUP BY time
                    UNION ALL
                    SELECT minute AS time, 'sportsbook' AS vertical,
                        sum({eur("stake")}) AS turnover_eur
                    FROM delta1.sport_ggr_1m WHERE $__timeFilter(minute) GROUP BY time
                ) ORDER BY time""",
                TIMESERIES,
            )
        ],
        12,
        8,
        unit="currencyEUR",
        custom=LINE,
        options=TS_OPTS,
    )

    b.row("Game aggregator")
    b.add(
        "table",
        "Top games: actual vs theoretical RTP",
        [
            ch(
                f"""SELECT g.title AS game, a.provider AS provider, a.n_rounds AS rounds,
                    a.t_eur AS turnover_eur, a.t_eur - a.p_eur AS ggr_eur,
                    a.p_eur / a.t_eur AS rtp_actual, g.rtp AS rtp_theoretical,
                    a.p_eur / a.t_eur - g.rtp AS rtp_delta
                FROM (
                    SELECT game_id, provider, sum(rounds) AS n_rounds,
                        sum({eur("turnover")}) AS t_eur,
                        sum({eur("payout")}) AS p_eur
                    FROM delta1.casino_ggr_1m WHERE $__timeFilter(minute)
                    GROUP BY game_id, provider
                ) AS a
                LEFT JOIN delta1.games AS g ON g.game_id = a.game_id
                ORDER BY turnover_eur DESC LIMIT 12"""
            )
        ],
        24,
        10,
        options={"showHeader": True, "cellHeight": "sm", "footer": {"show": False}},
        overrides=[
            _unit_override("turnover_eur", "currencyEUR", 0, "Turnover"),
            _unit_override("ggr_eur", "currencyEUR", 0, "GGR"),
            _unit_override("rtp_actual", "percentunit", 2, "RTP actual"),
            _unit_override("rtp_theoretical", "percentunit", 2, "RTP theoretical"),
            {
                "matcher": {"id": "byName", "options": "rtp_delta"},
                "properties": [
                    {"id": "unit", "value": "percentunit"},
                    {"id": "decimals", "value": 2},
                    {"id": "displayName", "value": "RTP delta"},
                    {"id": "custom.cellOptions", "value": {"type": "color-text"}},
                    {"id": "color", "value": {"mode": "thresholds"}},
                    {
                        "id": "thresholds",
                        "value": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "#73BF69", "value": None},
                                {"color": "#FF9830", "value": 0.02},
                                {"color": "#F2495C", "value": 0.05},
                            ],
                        },
                    },
                ],
            },
        ],
        description="Large positive deltas mean a game paid out more than its math model. "
        "In production that is the first thing to check with the provider.",
    )
    b.add(
        "barchart",
        "GGR by provider",
        [
            ch(
                f"""SELECT provider, sum({eur("turnover - payout")}) AS ggr
                FROM delta1.casino_ggr_1m WHERE $__timeFilter(minute)
                GROUP BY provider ORDER BY ggr DESC"""
            )
        ],
        24,
        7,
        unit="currencyEUR",
        decimals=0,
        options={
            "orientation": "horizontal",
            "showValue": "auto",
            "legend": {"showLegend": False},
            "xField": "provider",
        },
        custom={"fillOpacity": 80, "lineWidth": 0},
        color_mode="fixed",
    )

    b.row("Sportsbook")
    b.add(
        "piechart",
        "Stake by sport",
        [
            ch(
                f"""SELECT sport, sum({eur("stake")}) AS stake_eur
                FROM delta1.sport_ggr_1m WHERE $__timeFilter(minute)
                GROUP BY sport ORDER BY stake_eur DESC"""
            )
        ],
        6,
        9,
        unit="currencyEUR",
        decimals=0,
        options={
            "pieType": "donut",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": True},
            "legend": {"displayMode": "table", "placement": "right", "values": ["percent"]},
        },
    )
    b.add(
        "table",
        "Settled bets by sport",
        [
            ch(
                f"""SELECT sport, sum(bets) AS settled,
                    sumIf(bets, status = 'won') / sum(bets) AS win_rate,
                    sum({eur("stake")}) AS stake_eur,
                    1 - sum({eur("payout")}) / sum({eur("stake")}) AS hold
                FROM delta1.sport_ggr_1m WHERE $__timeFilter(minute)
                GROUP BY sport ORDER BY stake_eur DESC"""
            )
        ],
        12,
        9,
        options={"showHeader": True, "cellHeight": "sm"},
        overrides=[
            _unit_override("win_rate", "percentunit", 1, "Win rate"),
            _unit_override("stake_eur", "currencyEUR", 0, "Stake"),
            _unit_override("hold", "percentunit", 1, "Hold"),
        ],
        description="Hold = 1 - payout / stake. The simulator prices bets with a 6% margin.",
    )
    b.add(
        "stat",
        "Open bets (liability)",
        [
            ch(
                f"""SELECT sum({eur("stake * odds")}) AS liability
                FROM delta1.sport_bets FINAL WHERE status = 'open'"""
            )
        ],
        6,
        9,
        unit="currencyEUR",
        decimals=0,
        thresholds=ORANGE,
        options={**STAT, "graphMode": "none"},
        description="Potential payout of unsettled bets. Reads the ReplacingMergeTree with "
        "FINAL to get the latest version of each bet.",
    )

    b.row("Payments")
    b.add(
        "timeseries",
        "Deposits per minute by method",
        [
            ch(
                f"""SELECT toStartOfMinute(ts) AS time, method, sum({eur("amount")}) AS amount
                FROM delta1.deposits WHERE $__timeFilter(ts)
                GROUP BY time, method ORDER BY time""",
                TIMESERIES,
            )
        ],
        16,
        8,
        unit="currencyEUR",
        custom={**STACKED, "drawStyle": "bars", "fillOpacity": 80},
        options=TS_OPTS,
    )
    b.add(
        "stat",
        "First-time depositors",
        [ch("SELECT countIf(is_first) AS ftd FROM delta1.deposits WHERE $__timeFilter(ts)")],
        8,
        8,
        thresholds=GREEN,
        options=STAT,
    )
    return b


def _unit_override(name: str, unit: str, decimals: int, label: str | None = None) -> dict:
    props = [{"id": "unit", "value": unit}, {"id": "decimals", "value": decimals}]
    if label:
        props.append({"id": "displayName", "value": label})
    return {"matcher": {"id": "byName", "options": name}, "properties": props}


# --------------------------------------------------------------------------- pipeline
def pipeline() -> Board:
    b = Board("delta1-pipeline", "Delta1 · Pipeline health", refresh="5s", time_from="now-15m")

    b.row("Throughput")
    b.add(
        "stat",
        "Consumer lag",
        [prom("sum(delta1_processor_consumer_lag)")],
        6,
        4,
        thresholds=[
            {"color": "#73BF69", "value": None},
            {"color": "#FF9830", "value": 10000},
            {"color": "#F2495C", "value": 100000},
        ],
        options={**STAT, "graphMode": "area"},
        description="Messages in Kafka not yet written to ClickHouse.",
    )
    b.add(
        "stat",
        "Events in / s",
        [prom("sum(rate(delta1_processor_consumed_total[1m]))")],
        6,
        4,
        unit="suffix: ev/s",
        decimals=0,
        thresholds=BLUE,
        options={**STAT, "graphMode": "area"},
    )
    b.add(
        "stat",
        "Flush p95",
        [
            prom(
                "histogram_quantile(0.95, sum by (le) "
                "(rate(delta1_processor_flush_seconds_bucket[1m])))"
            )
        ],
        6,
        4,
        unit="s",
        decimals=3,
        thresholds=PURPLE,
        options={**STAT, "graphMode": "area"},
        description="Time to write one batch (all tables) to ClickHouse.",
    )
    b.add(
        "stat",
        "DLQ share",
        [
            prom(
                "sum(rate(delta1_processor_invalid_total[5m])) / "
                "sum(rate(delta1_processor_consumed_total[5m]))"
            )
        ],
        6,
        4,
        unit="percentunit",
        decimals=2,
        thresholds=[
            {"color": "#73BF69", "value": None},
            {"color": "#FF9830", "value": 0.01},
            {"color": "#F2495C", "value": 0.05},
        ],
        options={**STAT, "graphMode": "area"},
    )

    b.add(
        "timeseries",
        "Produced vs consumed vs accepted",
        [
            prom("sum(rate(delta1_generator_events_total[1m]))", "produced"),
            prom("sum(rate(delta1_processor_consumed_total[1m]))", "consumed", "B"),
            prom("sum(rate(delta1_processor_accepted_total[1m]))", "accepted", "C"),
        ],
        12,
        8,
        unit="suffix: ev/s",
        custom=LINE,
        options=TS_OPTS,
    )
    b.add(
        "timeseries",
        "Rows written to ClickHouse by table",
        [
            prom(
                "sum by (table) (rate(delta1_processor_rows_written_total[1m]))",
                "{{table}}",
            )
        ],
        12,
        8,
        unit="suffix: rows/s",
        custom=STACKED,
        options=TS_OPTS,
    )

    b.row("Data quality")
    b.add(
        "timeseries",
        "Dead letters by reason",
        [prom("sum by (reason) (rate(delta1_processor_invalid_total[1m]))", "{{reason}}")],
        8,
        8,
        unit="suffix: ev/s",
        custom=STACKED,
        options=TS_OPTS,
    )
    b.add(
        "timeseries",
        "Duplicates dropped",
        [
            prom("sum(rate(delta1_processor_duplicates_total[1m]))", "dropped"),
            prom(
                "sum(rate(delta1_generator_injected_total{kind='duplicate'}[1m]))", "injected", "B"
            ),
        ],
        8,
        8,
        unit="suffix: ev/s",
        custom=LINE,
        options=TS_OPTS,
        description="Injected by the generator vs dropped by the processor: the lines should "
        "overlap.",
    )
    b.add(
        "table",
        "Latest dead letters",
        [
            ch(
                """SELECT received_ts AS time, reason, substring(payload, 1, 120) AS payload
                FROM delta1.dead_letters WHERE $__timeFilter(received_ts)
                ORDER BY received_ts DESC LIMIT 50"""
            )
        ],
        8,
        8,
        options={"showHeader": True, "cellHeight": "sm"},
    )

    b.row("ClickHouse")
    b.add(
        "timeseries",
        "Inserted rows / s (server side)",
        [prom("rate(ClickHouseProfileEvents_InsertedRows[1m])", "inserted rows")],
        8,
        8,
        unit="suffix: rows/s",
        custom=LINE,
        options=TS_OPTS,
    )
    b.add(
        "timeseries",
        "Active parts and merges",
        [
            prom("ClickHouseAsyncMetrics_TotalPartsOfMergeTreeTables", "active parts"),
            prom("ClickHouseMetrics_Merge", "running merges", "B"),
        ],
        8,
        8,
        custom=LINE,
        options=TS_OPTS,
        description="Many tiny inserts would show up here as a growing part count. "
        "Batching keeps it flat.",
    )
    b.add(
        "timeseries",
        "Memory tracked",
        [prom("ClickHouseMetrics_MemoryTracking", "memory")],
        8,
        8,
        unit="bytes",
        custom=LINE,
        options=TS_OPTS,
    )
    return b


# --------------------------------------------------------------------------- batch
def batch() -> Board:
    b = Board("delta1-batch", "Delta1 · Partners and batch", refresh="30s", time_from="now-3h")

    recon = "FROM delta1.reconciliation FINAL WHERE $__timeFilter(window_start)"
    red_if_any = [{"color": "#73BF69", "value": None}, {"color": "#F2495C", "value": 1}]

    b.row("Provider reconciliation")
    b.add(
        "stat",
        "Match rate",
        [ch(f"SELECT countIf(status = 'ok') / count() AS match_rate {recon}")],
        6,
        4,
        unit="percentunit",
        decimals=1,
        thresholds=[
            {"color": "#F2495C", "value": None},
            {"color": "#FF9830", "value": 0.9},
            {"color": "#73BF69", "value": 0.98},
        ],
        options=STAT,
        description="Share of (window, provider, game, currency) rows where provider "
        "numbers match ours within 0.1%.",
    )
    b.add(
        "stat",
        "Windows reconciled",
        [ch(f"SELECT uniqExact(window_start) AS windows {recon}")],
        6,
        4,
        thresholds=BLUE,
        options=STAT,
    )
    b.add(
        "stat",
        "Amount mismatches",
        [ch(f"SELECT countIf(status = 'mismatch') AS mismatches {recon}")],
        6,
        4,
        thresholds=red_if_any,
        options=STAT,
    )
    b.add(
        "stat",
        "Missing rows",
        [ch(f"SELECT countIf(status LIKE 'missing%') AS missing {recon}")],
        6,
        4,
        thresholds=red_if_any,
        options=STAT,
        description="Rows that exist on only one side: we recorded play the provider did "
        "not report, or the other way round.",
    )
    b.add(
        "timeseries",
        "Reconciliation status per window",
        [
            ch(
                f"""SELECT window_start AS time, status, count() AS row_count {recon}
                GROUP BY time, status ORDER BY time""",
                TIMESERIES,
            )
        ],
        24,
        7,
        custom={**STACKED, "drawStyle": "bars", "fillOpacity": 80},
        options=TS_OPTS,
        overrides=[
            _color_override("row_count ok", "#73BF69"),
            _color_override("row_count mismatch", "#F2495C"),
            _color_override("row_count missing_in_report", "#FF9830"),
            _color_override("row_count missing_on_platform", "#B877D9"),
        ],
    )
    b.add(
        "table",
        "Open discrepancies",
        [
            ch(
                f"""SELECT window_start AS window, provider, game_id AS game, currency, status,
                    platform_bets, provider_bets, bets_diff_pct
                {recon} AND status != 'ok'
                ORDER BY window_start DESC LIMIT 50"""
            )
        ],
        24,
        8,
        options={"showHeader": True, "cellHeight": "sm"},
        overrides=[
            _unit_override("bets_diff_pct", "percentunit", 2, "Bets diff"),
            _unit_override("platform_bets", "none", 2, "Our bets"),
            _unit_override("provider_bets", "none", 2, "Their bets"),
        ],
        description="Amounts in the original currency. Each row is something to raise "
        "with the provider before invoicing.",
    )

    b.row("Daily marts (today)")
    b.add(
        "stat",
        "Mart freshness",
        [ch("SELECT dateDiff('second', max(built_at), now()) AS age FROM delta1.player_daily")],
        6,
        4,
        unit="s",
        thresholds=[
            {"color": "#73BF69", "value": None},
            {"color": "#FF9830", "value": 1800},
            {"color": "#F2495C", "value": 3600},
        ],
        options=STAT,
        description="Time since the daily marts DAG last published (every 15 min by default).",
    )
    b.add(
        "stat",
        "Players in today's mart",
        [ch("SELECT count() AS players FROM delta1.player_daily WHERE day = today()")],
        6,
        4,
        thresholds=BLUE,
        options=STAT,
    )
    b.add(
        "stat",
        "GGR today (mart)",
        [ch("SELECT sum(ggr_eur) AS ggr FROM delta1.player_daily WHERE day = today()")],
        6,
        4,
        unit="currencyEUR",
        decimals=0,
        thresholds=GREEN,
        options=STAT,
    )
    b.add(
        "stat",
        "Depositing players today",
        [
            ch(
                "SELECT countIf(deposits > 0) / count() AS share FROM delta1.player_daily "
                "WHERE day = today()"
            )
        ],
        6,
        4,
        unit="percentunit",
        decimals=1,
        thresholds=ORANGE,
        options=STAT,
    )
    b.add(
        "table",
        "Top players by GGR today",
        [
            ch(
                """SELECT player_id, brand, casino_rounds AS rounds, sport_bets_placed AS bets,
                    ggr_eur
                FROM delta1.player_daily WHERE day = today()
                ORDER BY ggr_eur DESC LIMIT 15"""
            )
        ],
        12,
        10,
        options={"showHeader": True, "cellHeight": "sm"},
        overrides=[_unit_override("ggr_eur", "currencyEUR", 2, "GGR")],
    )
    b.add(
        "table",
        "Provider daily: RTP by game",
        [
            ch(
                """SELECT provider, game_id AS game, sum(rounds) AS rounds, sum(ggr_eur) AS ggr,
                    1 - sum(ggr_eur) / sum(turnover_eur) AS rtp
                FROM delta1.provider_daily WHERE day = today()
                GROUP BY provider, game ORDER BY ggr DESC"""
            )
        ],
        12,
        10,
        options={"showHeader": True, "cellHeight": "sm"},
        overrides=[
            _unit_override("ggr", "currencyEUR", 0, "GGR"),
            _unit_override("rtp", "percentunit", 2, "RTP"),
        ],
        description="Built from the provider_daily mart; RTP is EUR-weighted across currencies.",
    )

    b.row("FX rates (partner API, hourly)")
    for currency, w in (("USD", 12), ("RSD", 12)):
        b.add(
            "timeseries",
            f"EUR per 1 {currency}",
            [
                ch(
                    f"""SELECT as_of AS time, argMax(rate_to_eur, loaded_at) AS rate
                    FROM delta1.fx_rates_history
                    WHERE currency = '{currency}' AND $__timeFilter(as_of)
                    GROUP BY time ORDER BY time""",
                    TIMESERIES,
                )
            ],
            w,
            7,
            decimals=6,
            custom={**LINE, "showPoints": "always", "pointSize": 6},
            options={**TS_OPTS, "legend": {"showLegend": False}},
        )
    return b


def _color_override(name: str, color: str) -> dict:
    return {
        "matcher": {"id": "byName", "options": name},
        "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}],
    }


def main() -> None:
    OUT.mkdir(exist_ok=True)
    for board in (business(), pipeline(), batch()):
        name = board.uid.removeprefix("delta1-")
        (OUT / f"{name}.json").write_text(json.dumps(board.json(), indent=2) + "\n")
        print(f"wrote {OUT / name}.json ({len(board.panels)} panels)")


if __name__ == "__main__":
    main()
