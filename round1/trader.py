import json
from dataclasses import dataclass
from math import ceil, floor
from typing import Dict, List, Optional, Tuple

from datamodel import Order, OrderDepth, TradingState


PEPPER = "INTARIAN_PEPPER_ROOT"
OSMIUM = "ASH_COATED_OSMIUM"


@dataclass
class BookSnapshot:
    best_bid: Optional[int]
    best_bid_volume: int
    best_ask: Optional[int]
    best_ask_volume: int
    mid: float
    microprice: float
    imbalance: float
    buys: List[Tuple[int, int]]
    sells: List[Tuple[int, int]]


class OrderPlanner:
    def __init__(self, product: str, limit: int, position: int) -> None:
        self.product = product
        self.limit = limit
        self.position = position
        self.orders: List[Order] = []
        self.buy_reserved = 0
        self.sell_reserved = 0

    def remaining_buy(self) -> int:
        return max(0, self.limit - self.position - self.buy_reserved)

    def remaining_sell(self) -> int:
        return max(0, self.limit + self.position - self.sell_reserved)

    def buy(self, price: int, quantity: int) -> None:
        size = min(max(0, quantity), self.remaining_buy())
        if size <= 0:
            return
        self.orders.append(Order(self.product, int(price), int(size)))
        self.buy_reserved += size

    def sell(self, price: int, quantity: int) -> None:
        size = min(max(0, quantity), self.remaining_sell())
        if size <= 0:
            return
        self.orders.append(Order(self.product, int(price), -int(size)))
        self.sell_reserved += size


class Trader:
    POSITION_LIMITS = {
        PEPPER: 80,
        OSMIUM: 80,
    }

    DAY_END_TIMESTAMP = 1_000_000
    PEPPER_SLOPE = 0.001
    PEPPER_DECAY_POWER = 0.7

    def run(self, state: TradingState):
        cache = self._load_cache(state.traderData)
        self._reset_cache_for_new_day(cache, state.timestamp)
        result: Dict[str, List[Order]] = {}

        for product, depth in state.order_depths.items():
            if product not in self.POSITION_LIMITS:
                continue

            snapshot = self._snapshot(depth)
            if snapshot is None:
                result[product] = []
                continue

            position = state.position.get(product, 0)
            planner = OrderPlanner(product, self.POSITION_LIMITS[product], position)

            if product == PEPPER:
                fair_value = self._pepper_fair_value(state.timestamp, snapshot, cache)
                target_position = self._pepper_target_position(state.timestamp)
                reservation = fair_value + 0.20 * (target_position - position)
                self._trade_product(
                    planner=planner,
                    snapshot=snapshot,
                    reservation=reservation,
                    take_width=0.5,
                    quote_edge=2.0,
                    max_passive_size=24,
                )
            else:
                fair_value = self._osmium_fair_value(snapshot, cache)
                target_position = 0
                reservation = fair_value + 0.12 * (target_position - position)
                self._trade_product(
                    planner=planner,
                    snapshot=snapshot,
                    reservation=reservation,
                    take_width=0.75,
                    quote_edge=2.0,
                    max_passive_size=20,
                )

            result[product] = planner.orders

        cache["meta"]["last_timestamp"] = state.timestamp
        trader_data = json.dumps(cache, separators=(",", ":"))
        return result, 0, trader_data

    def _default_cache(self) -> Dict[str, Dict[str, float]]:
        return {
            "meta": {"last_timestamp": -1},
            PEPPER: {"anchor": 0.0, "seen": 0},
            OSMIUM: {"ema_short": 0.0, "ema_long": 0.0, "seen": 0},
        }

    def _load_cache(self, trader_data: str) -> Dict[str, Dict[str, float]]:
        default_cache = self._default_cache()
        if not trader_data:
            return default_cache

        try:
            loaded = json.loads(trader_data)
        except json.JSONDecodeError:
            return default_cache

        meta_cache = loaded.get("meta", {})
        meta_defaults = default_cache["meta"]
        for key, value in meta_defaults.items():
            meta_cache.setdefault(key, value)
        loaded["meta"] = meta_cache

        for product, defaults in default_cache.items():
            if product == "meta":
                continue
            product_cache = loaded.get(product, {})
            for key, value in defaults.items():
                product_cache.setdefault(key, value)
            loaded[product] = product_cache

        return loaded

    def _reset_cache_for_new_day(
        self,
        cache: Dict[str, Dict[str, float]],
        timestamp: int,
    ) -> None:
        last_timestamp = cache["meta"].get("last_timestamp", -1)
        if last_timestamp != -1 and timestamp < last_timestamp:
            fresh_cache = self._default_cache()
            cache[PEPPER] = fresh_cache[PEPPER]
            cache[OSMIUM] = fresh_cache[OSMIUM]

    def _snapshot(self, depth: OrderDepth) -> Optional[BookSnapshot]:
        buys = sorted(depth.buy_orders.items(), key=lambda item: item[0], reverse=True)
        sells = sorted(depth.sell_orders.items(), key=lambda item: item[0])
        if not buys and not sells:
            return None

        best_bid = buys[0][0] if buys else None
        best_bid_volume = buys[0][1] if buys else 0
        best_ask = sells[0][0] if sells else None
        best_ask_volume = -sells[0][1] if sells else 0

        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2
            volume_sum = best_bid_volume + best_ask_volume
            if volume_sum > 0:
                microprice = (
                    best_bid * best_ask_volume + best_ask * best_bid_volume
                ) / volume_sum
                imbalance = (best_bid_volume - best_ask_volume) / volume_sum
            else:
                microprice = mid
                imbalance = 0.0
        elif best_bid is not None:
            mid = float(best_bid)
            microprice = mid
            imbalance = 0.0
        else:
            mid = float(best_ask)
            microprice = mid
            imbalance = 0.0

        return BookSnapshot(
            best_bid=best_bid,
            best_bid_volume=best_bid_volume,
            best_ask=best_ask,
            best_ask_volume=best_ask_volume,
            mid=mid,
            microprice=microprice,
            imbalance=imbalance,
            buys=buys,
            sells=sells,
        )

    def _pepper_fair_value(
        self,
        timestamp: int,
        snapshot: BookSnapshot,
        cache: Dict[str, Dict[str, float]],
    ) -> float:
        pepper_cache = cache[PEPPER]
        observed_anchor = snapshot.mid - self.PEPPER_SLOPE * timestamp

        if pepper_cache["seen"] == 0:
            anchor = observed_anchor
        else:
            anchor = 0.85 * pepper_cache["anchor"] + 0.15 * observed_anchor

        pepper_cache["anchor"] = anchor
        pepper_cache["seen"] += 1

        return anchor + self.PEPPER_SLOPE * timestamp + 4.0 * snapshot.imbalance

    def _pepper_target_position(self, timestamp: int) -> int:
        remaining_fraction = max(
            0.0, 1.0 - (timestamp / float(self.DAY_END_TIMESTAMP))
        )
        remaining_fraction = remaining_fraction ** self.PEPPER_DECAY_POWER
        return int(round(self.POSITION_LIMITS[PEPPER] * remaining_fraction))

    def _osmium_fair_value(
        self,
        snapshot: BookSnapshot,
        cache: Dict[str, Dict[str, float]],
    ) -> float:
        osmium_cache = cache[OSMIUM]
        mid = snapshot.mid

        if osmium_cache["seen"] == 0:
            ema_short = mid
            ema_long = mid
        else:
            ema_short = 0.35 * mid + 0.65 * osmium_cache["ema_short"]
            ema_long = 0.08 * mid + 0.92 * osmium_cache["ema_long"]

        osmium_cache["ema_short"] = ema_short
        osmium_cache["ema_long"] = ema_long
        osmium_cache["seen"] += 1

        mean_reversion_signal = 0.55 * (ema_short - mid) + 0.25 * (ema_long - mid)
        imbalance_signal = 4.5 * snapshot.imbalance
        predicted_move = self._clamp(
            mean_reversion_signal + imbalance_signal,
            -6.0,
            6.0,
        )
        return mid + predicted_move

    def _trade_product(
        self,
        planner: OrderPlanner,
        snapshot: BookSnapshot,
        reservation: float,
        take_width: float,
        quote_edge: float,
        max_passive_size: int,
    ) -> None:
        for ask_price, ask_volume in snapshot.sells:
            size = -ask_volume
            if ask_price > reservation - take_width:
                break
            planner.buy(ask_price, size)

        for bid_price, bid_volume in snapshot.buys:
            if bid_price < reservation + take_width:
                break
            planner.sell(bid_price, bid_volume)

        passive_bid = self._passive_bid_price(snapshot, reservation, quote_edge)
        passive_ask = self._passive_ask_price(snapshot, reservation, quote_edge)

        if passive_bid is not None and planner.remaining_buy() > 0:
            planner.buy(passive_bid, min(max_passive_size, planner.remaining_buy()))

        if passive_ask is not None and planner.remaining_sell() > 0:
            planner.sell(passive_ask, min(max_passive_size, planner.remaining_sell()))

    def _passive_bid_price(
        self,
        snapshot: BookSnapshot,
        reservation: float,
        quote_edge: float,
    ) -> Optional[int]:
        ideal_price = int(floor(reservation - quote_edge))

        if snapshot.best_bid is not None:
            if snapshot.best_ask is not None and snapshot.best_bid + 1 < snapshot.best_ask:
                ideal_price = min(ideal_price, snapshot.best_bid + 1)
            else:
                ideal_price = min(ideal_price, snapshot.best_bid)

        if snapshot.best_ask is not None:
            ideal_price = min(ideal_price, snapshot.best_ask - 1)

        return ideal_price

    def _passive_ask_price(
        self,
        snapshot: BookSnapshot,
        reservation: float,
        quote_edge: float,
    ) -> Optional[int]:
        ideal_price = int(ceil(reservation + quote_edge))

        if snapshot.best_ask is not None:
            if snapshot.best_bid is not None and snapshot.best_ask - 1 > snapshot.best_bid:
                ideal_price = max(ideal_price, snapshot.best_ask - 1)
            else:
                ideal_price = max(ideal_price, snapshot.best_ask)

        if snapshot.best_bid is not None:
            ideal_price = max(ideal_price, snapshot.best_bid + 1)

        return ideal_price

    def _clamp(self, value: float, lower: float, upper: float) -> float:
        return max(lower, min(upper, value))
