from datamodel import Order, OrderDepth, TradingState
from typing import Dict, List
import jsonpickle
import math

POSITION_LIMIT = {
    "EMERALDS": 80,
    "TOMATOES": 80,
}

class Trader:

    def load_data(self, state: TradingState):
        if state.traderData:
            try:
                return jsonpickle.decode(state.traderData)
            except:
                return {"mid_hist": {"EMERALDS": [], "TOMATOES": []}}
        return {"mid_hist": {"EMERALDS": [], "TOMATOES": []}}

    def save_data(self, data):
        return jsonpickle.encode(data)

    def get_best_bid_ask(self, order_depth: OrderDepth):
        best_bid = max(order_depth.buy_orders.keys()) if order_depth.buy_orders else None
        best_ask = min(order_depth.sell_orders.keys()) if order_depth.sell_orders else None
        bid_vol = order_depth.buy_orders[best_bid] if best_bid is not None else 0
        ask_vol = -order_depth.sell_orders[best_ask] if best_ask is not None else 0
        return best_bid, bid_vol, best_ask, ask_vol

    def get_mid(self, order_depth: OrderDepth):
        best_bid, _, best_ask, _ = self.get_best_bid_ask(order_depth)
        if best_bid is not None and best_ask is not None:
            return (best_bid + best_ask) / 2
        elif best_bid is not None:
            return best_bid
        elif best_ask is not None:
            return best_ask
        return None

    def get_microprice(self, order_depth: OrderDepth):
        best_bid, bid_vol, best_ask, ask_vol = self.get_best_bid_ask(order_depth)
        if best_bid is None or best_ask is None or bid_vol + ask_vol == 0:
            return self.get_mid(order_depth)
        return (best_ask * bid_vol + best_bid * ask_vol) / (bid_vol + ask_vol)

    def get_imbalance(self, order_depth: OrderDepth):
        bid_total = sum(order_depth.buy_orders.values()) if order_depth.buy_orders else 0
        ask_total = -sum(order_depth.sell_orders.values()) if order_depth.sell_orders else 0
        total = bid_total + ask_total
        if total == 0:
            return 0.0
        return (bid_total - ask_total) / total

    def moving_average(self, arr, window):
        if not arr:
            return None
        if len(arr) < window:
            return sum(arr) / len(arr)
        return sum(arr[-window:]) / window

    def max_buyable(self, product, position):
        return POSITION_LIMIT[product] - position

    def max_sellable(self, product, position):
        return POSITION_LIMIT[product] + position

    def take_liquidity_buy(self, product, order_depth, fair, max_qty):
        orders = []
        if max_qty <= 0:
            return orders
        for ask_price in sorted(order_depth.sell_orders.keys()):
            ask_volume = -order_depth.sell_orders[ask_price]
            if ask_price <= fair - 1 and max_qty > 0:
                qty = min(ask_volume, max_qty)
                if qty > 0:
                    orders.append(Order(product, ask_price, qty))
                    max_qty -= qty
        return orders

    def take_liquidity_sell(self, product, order_depth, fair, max_qty):
        orders = []
        if max_qty <= 0:
            return orders
        for bid_price in sorted(order_depth.buy_orders.keys(), reverse=True):
            bid_volume = order_depth.buy_orders[bid_price]
            if bid_price >= fair + 1 and max_qty > 0:
                qty = min(bid_volume, max_qty)
                if qty > 0:
                    orders.append(Order(product, bid_price, -qty))
                    max_qty -= qty
        return orders

    def make_market_emeralds(self, product, order_depth, fair, position):
        orders = []
        buy_cap = self.max_buyable(product, position)
        sell_cap = self.max_sellable(product, position)

        best_bid, _, best_ask, _ = self.get_best_bid_ask(order_depth)

        buy_size = 10
        sell_size = 10

        if position > 40:
            buy_size = 4
            sell_size = 16
        elif position < -40:
            buy_size = 16
            sell_size = 4

        bid_px = int(math.floor(fair - 1))
        ask_px = int(math.ceil(fair + 1))

        if best_bid is not None:
            bid_px = min(bid_px, best_bid + 1)
        if best_ask is not None:
            ask_px = max(ask_px, best_ask - 1)

        if buy_cap > 0 and position < 75:
            orders.append(Order(product, bid_px, min(buy_size, buy_cap)))
        if sell_cap > 0 and position > -75:
            orders.append(Order(product, ask_px, -min(sell_size, sell_cap)))

        return orders

    def make_market_tomatoes(self, product, order_depth, fair, position, trend, imbalance):
        orders = []
        buy_cap = self.max_buyable(product, position)
        sell_cap = self.max_sellable(product, position)

        best_bid, _, best_ask, _ = self.get_best_bid_ask(order_depth)
        if best_bid is None or best_ask is None:
            return orders

        base_size = 6
        if abs(trend) > 2:
            base_size = 10
        if abs(position) > 50:
            base_size = 4

        # 中性或轻微偏向时双边挂单；强趋势时偏单边
        if trend > 1 or imbalance > 0.15:
            if buy_cap > 0 and position < 70:
                orders.append(Order(product, min(best_bid + 1, int(fair - 1)), min(base_size, buy_cap)))
            if sell_cap > 0 and position > 0:
                orders.append(Order(product, max(best_ask - 1, int(fair + 2)), -min(base_size // 2 + 2, sell_cap)))
        elif trend < -1 or imbalance < -0.15:
            if sell_cap > 0 and position > -70:
                orders.append(Order(product, max(best_ask - 1, int(fair + 1)), -min(base_size, sell_cap)))
            if buy_cap > 0 and position < 0:
                orders.append(Order(product, min(best_bid + 1, int(fair - 2)), min(base_size // 2 + 2, buy_cap)))
        else:
            if buy_cap > 0 and position < 70:
                orders.append(Order(product, min(best_bid + 1, int(fair - 1)), min(base_size, buy_cap)))
            if sell_cap > 0 and position > -70:
                orders.append(Order(product, max(best_ask - 1, int(fair + 1)), -min(base_size, sell_cap)))

        return orders

    def run(self, state: TradingState):
        data = self.load_data(state)
        result: Dict[str, List[Order]] = {}

        for product, order_depth in state.order_depths.items():
            orders: List[Order] = []
            position = state.position.get(product, 0)
            mid = self.get_mid(order_depth)

            if mid is not None:
                data["mid_hist"].setdefault(product, []).append(mid)
                data["mid_hist"][product] = data["mid_hist"][product][-100:]

            if product == "EMERALDS":
                fair = 10000 - 0.1 * position

                buy_cap = min(20, self.max_buyable(product, position))
                sell_cap = min(20, self.max_sellable(product, position))

                orders += self.take_liquidity_buy(product, order_depth, fair, buy_cap)
                orders += self.take_liquidity_sell(product, order_depth, fair, sell_cap)

                # 更新剩余容量（粗略处理，防止过量）
                bought = sum(o.quantity for o in orders if o.quantity > 0)
                sold = -sum(o.quantity for o in orders if o.quantity < 0)
                temp_pos = position + bought - sold

                orders += self.make_market_emeralds(product, order_depth, fair, temp_pos)

            elif product == "TOMATOES":
                micro = self.get_microprice(order_depth)
                imbalance = self.get_imbalance(order_depth)

                hist = data["mid_hist"].get(product, [])
                short_ma = self.moving_average(hist, 5) if hist else micro
                long_ma = self.moving_average(hist, 20) if hist else micro
                if short_ma is None:
                    short_ma = micro
                if long_ma is None:
                    long_ma = short_ma

                trend = short_ma - long_ma
                raw_fair = 0.5 * micro + 0.3 * short_ma + 0.2 * long_ma
                fair = raw_fair + 1.5 * imbalance - 0.25 * position

                buy_cap = min(20, self.max_buyable(product, position))
                sell_cap = min(20, self.max_sellable(product, position))

                # 强信号先吃单
                orders += self.take_liquidity_buy(product, order_depth, fair, buy_cap)
                orders += self.take_liquidity_sell(product, order_depth, fair, sell_cap)

                bought = sum(o.quantity for o in orders if o.quantity > 0)
                sold = -sum(o.quantity for o in orders if o.quantity < 0)
                temp_pos = position + bought - sold

                # 仓位太大时只减仓
                if temp_pos > 60:
                    best_bid = max(order_depth.buy_orders.keys()) if order_depth.buy_orders else None
                    if best_bid is not None:
                        qty = min(15, self.max_sellable(product, temp_pos))
                        orders.append(Order(product, best_bid, -qty))
                elif temp_pos < -60:
                    best_ask = min(order_depth.sell_orders.keys()) if order_depth.sell_orders else None
                    if best_ask is not None:
                        qty = min(15, self.max_buyable(product, temp_pos))
                        orders.append(Order(product, best_ask, qty))
                else:
                    orders += self.make_market_tomatoes(product, order_depth, fair, temp_pos, trend, imbalance)

            result[product] = orders

        traderData = self.save_data(data)
        conversions = 0
        return result, conversions, traderData