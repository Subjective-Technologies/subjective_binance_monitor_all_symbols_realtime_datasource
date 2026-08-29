import sys
from collections import OrderedDict, deque
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from subjective_abstract_data_source_package import SubjectiveDataSource

from trading_contracts.plugin_support import icon_for, ticker_stream


class SubjectiveMonitorAllSymbolsRealtimeDataSource(SubjectiveDataSource):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.window = max(1, int(self._connection.get("window", 20)))
        self.max_symbols = max(1, int(self._connection.get("max_symbols", 200)))
        self._windows = OrderedDict()

    @classmethod
    def connection_schema(cls):
        return {
            "quote_filter": {"type": "text", "label": "Quote Filter", "default": "USDT"},
            "window": {"type": "int", "label": "Window", "default": 20, "min": 1},
            "max_symbols": {"type": "int", "label": "Max Symbols", "default": 200, "min": 1},
        }

    @classmethod
    def request_schema(cls):
        return {"events": {"type": "array", "label": "Market Events"}, "quote_filter": {"type": "text", "label": "Quote Filter"}}

    @classmethod
    def output_schema(cls):
        return {
            "event": {"type": "object", "label": "Market Event"},
            "sequence": {"type": "array", "label": "Price Sequence"},
            "sma": {"type": "text", "label": "SMA"},
            "tracked_symbols": {"type": "array", "label": "Tracked Symbols"},
            "error": {"type": "text", "label": "Error"},
        }

    @classmethod
    def icon(cls):
        return icon_for(__file__)

    def supports_streaming(self):
        return True

    def _with_sequence(self, event):
        symbol = event["symbol"]
        if symbol not in self._windows:
            if len(self._windows) >= self.max_symbols:
                return None
            self._windows[symbol] = deque(maxlen=self.window)
        values = self._windows[symbol]
        values.append(event["last"])
        sma = format(sum(Decimal(value) for value in values) / len(values), "f") if len(values) >= self.window else ""
        return {"event": event, "sequence": list(values), "sma": sma, "tracked_symbols": list(self._windows), "error": ""}

    def stream(self, request):
        for event in ticker_stream(request or {}, self._connection, "all"):
            if event.get("event") is None and event.get("error"):
                yield {"event": None, "sequence": [], "sma": "", "tracked_symbols": list(self._windows), "error": event["error"]}
                continue
            result = self._with_sequence(event)
            if result:
                yield result

    def run(self, request):
        result = {"event": None, "sequence": [], "sma": "", "tracked_symbols": list(self._windows), "error": ""}
        for result in self.stream(request or {}):
            pass
        return result
