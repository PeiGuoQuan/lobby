# orderbook.py
# -------------
# Price–time priority order book with IOC/FOK/Post-Only and Iceberg support.
# Standalone minimal implementation used by tests in `tests/test_tif_postonly.py` and `tests/test_iceberg.py`.

from collections import deque

# ========== Internal data structures ==========

class _Order:
    __slots__ = ("idNum", "tid", "price", "qty", "timestamp")
    def __init__(self, idNum, tid, price, qty, timestamp):
        self.idNum = int(idNum)
        self.tid = tid
        self.price = int(price)  # internal integer price
        self.qty = int(qty)
        self.timestamp = timestamp

class _PriceLevel:
    def __init__(self, price):
        self.price = int(price)
        self.queue = deque()  # FIFO Queue of _Order
        self.volume = 0

    def is_empty(self):
        return not self.queue

    def head(self):
        return self.queue[0] if self.queue else None

    # test helper compatibility
    def getHeadOrder(self):
        return self.head()

    def append(self, order: "_Order"):
        self.queue.append(order)
        self.volume += order.qty

    def consume_head(self, need: int):
        """Consume from head up to 'need'. Return (taken_qty, removed_order_or_None)."""
        if not self.queue or need <= 0:
            return 0, None
        head = self.queue[0]
        take = min(need, head.qty)
        head.qty -= take
        self.volume -= take
        removed = None
        if head.qty == 0:
            removed = self.queue.popleft()
        return take, removed

class _BookSide:
    """Minimal price-time priority container with ascending prices list."""
    def __init__(self):
        self.priceMap = {}  # price -> _PriceLevel
        self.prices = []    # sorted ascending list of int prices
        self.orderMap = {}  # idNum -> _Order
        self.nOrders = 0

    def _ensure_level(self, price: int):
        if price in self.priceMap:
            return self.priceMap[price]
        # insert into sorted list
        lvl = _PriceLevel(price)
        self.priceMap[price] = lvl
        inserted = False
        for i, p in enumerate(self.prices):
            if price < p:
                self.prices.insert(i, price)
                inserted = True
                break
        if not inserted:
            self.prices.append(price)
        return lvl

    def _prune_if_empty(self, price: int):
        lvl = self.priceMap.get(price)
        if lvl and lvl.is_empty():
            del self.priceMap[price]
            for i, p in enumerate(self.prices):
                if p == price:
                    self.prices.pop(i)
                    break

    def best_price_min(self):  # for asks
        return self.prices[0] if self.prices else None

    def best_price_max(self):  # for bids
        return self.prices[-1] if self.prices else None

    def level_at_min(self):
        p = self.best_price_min()
        return self.priceMap.get(p) if p is not None else None

    def level_at_max(self):
        p = self.best_price_max()
        return self.priceMap.get(p) if p is not None else None

    # compatibility with tests
    def priceExists(self, p:int):
        return p in self.priceMap

    def getPrice(self, p:int):
        return self.priceMap.get(p)

    def insert_order(self, qdict):
        price = int(qdict['price'])
        order = _Order(qdict['idNum'], qdict['tid'], price, int(qdict['qty']), qdict['timestamp'])
        lvl = self._ensure_level(price)
        lvl.append(order)
        self.orderMap[order.idNum] = order
        self.nOrders += 1
        return order

    def remove_by_id(self, idNum: int):
        order = self.orderMap.pop(idNum, None)
        if order is None:
            raise KeyError(idNum)
        # remove from its level (linear scan of head-first queue)
        lvl = self.priceMap.get(order.price)
        if lvl:
            newq = deque()
            removed = False
            while lvl.queue:
                o = lvl.queue.popleft()
                if not removed and o.idNum == idNum:
                    lvl.volume -= o.qty
                    removed = True
                else:
                    newq.append(o)
            lvl.queue = newq
            self._prune_if_empty(order.price)
        self.nOrders -= 1
        return order


# ========== Public OrderBook ==========

class OrderBook:
    def __init__(self, price_digits=3):
        self.tape = deque(maxlen=None)
        self.bids = _BookSide()
        self.asks = _BookSide()
        self.price_digits = int(price_digits)
        self._mult = 10 ** self.price_digits
        self.time = 0
        self.nextQuoteID = 1
        # iceberg registry: current *visible* id -> info
        # info = {'price': int, 'display': int, 'remaining': int, 'tid': any}
        self.icebergs = {}

    # ----- price helpers / queries -----
    def clipPrice(self, price_float):
        return int(round(float(price_float) * self._mult))

    def toFloatPrice(self, price_int):
        return float(price_int) / self._mult

    def getBestAsk(self):
        return self.asks.best_price_min()

    def getBestBid(self):
        return self.bids.best_price_max()

    def getVolumeAtPrice(self, side, price_float):
        ip = self.clipPrice(price_float)
        side = side.lower()
        tree = self.bids if side == 'bid' else self.asks
        lvl = tree.priceMap.get(ip)
        return 0 if lvl is None else int(lvl.volume)

    # ----- core entry -----
    def processOrder(self, quote, fromData=False, verbose=False):
        # timestamp & id
        if 'timestamp' not in quote:
            quote['timestamp'] = self.time
        self.time += 1
        if 'idNum' not in quote:
            quote['idNum'] = self.nextQuoteID
            self.nextQuoteID += 1

        typ = quote.get('type', 'limit').lower()
        if typ == 'market':
            trades = self._process_market(quote, verbose)
            return trades, None
        else:
            # limit: clip price to internal int
            quote['price'] = self.clipPrice(quote['price'])
            trades, order_in_book = self._process_limit(quote, verbose)
            return trades, order_in_book

    # ----- helpers -----
    def _record_trade(self, qty, price_ip, taker_tid, maker_tid):
        self.tape.append({'qty': int(qty), 'price': self.toFloatPrice(price_ip),
                          'taker_tid': taker_tid, 'maker_tid': maker_tid})

    def _is_marketable(self, side, price_ip):
        if side == 'bid':
            ba = self.getBestAsk()
            return ba is not None and price_ip >= ba
        else:
            bb = self.getBestBid()
            return bb is not None and price_ip <= bb

    def _sum_depth_fok(self, taker_side, limit_price_ip):
        total = 0
        if taker_side == 'bid':
            # eat asks <= price
            idx = 0
            while idx < len(self.asks.prices) and self.asks.prices[idx] <= limit_price_ip:
                p = self.asks.prices[idx]
                total += self.asks.priceMap[p].volume
                idx += 1
        else:
            # eat bids >= price
            idx = len(self.bids.prices) - 1
            while idx >= 0 and self.bids.prices[idx] >= limit_price_ip:
                p = self.bids.prices[idx]
                total += self.bids.priceMap[p].volume
                idx -= 1
        return total

    def _reprice_to_passive(self, side, price_ip):
        if side == 'bid':
            best_bid = self.getBestBid()
            return best_bid if best_bid is not None else price_ip
        else:
            best_ask = self.getBestAsk()
            return best_ask if best_ask is not None else price_ip

    # ----- matching primitives -----
    def _process_price_level(self, book_side_str, level: _PriceLevel, qtyToTrade, taker_quote):
        """book_side_str = 'ask' means we are consuming asks (taker is bid)."""
        trades = []
        tree = self.asks if book_side_str == 'ask' else self.bids
        while qtyToTrade > 0 and level and not level.is_empty():
            taken, removed = level.consume_head(qtyToTrade)
            if taken == 0:
                break
            qtyToTrade -= taken
            # record trade (price is level.price)
            maker_tid = removed.tid if removed else (level.head().tid if level.head() else taker_quote['tid'])
            self._record_trade(taken, level.price, taker_quote['tid'], maker_tid)
            trades.append({'qty': int(taken), 'price': self.toFloatPrice(level.price)})
            if removed:
                # clean mapping, prune empty level
                if removed.idNum in tree.orderMap:
                    del tree.orderMap[removed.idNum]
                    tree.nOrders -= 1
                if level.is_empty():
                    if level.price in tree.priceMap:
                        del tree.priceMap[level.price]
                        for i, p in enumerate(tree.prices):
                            if p == level.price:
                                tree.prices.pop(i)
                                break
                # iceberg replenish with NEW id
                self._iceberg_replenish_if_needed(book_side_str, removed)
        return qtyToTrade, trades

    def _iceberg_replenish_if_needed(self, book_side, removed_order: _Order):
        old_id = removed_order.idNum
        info = self.icebergs.pop(old_id, None)
        if not info or info['remaining'] <= 0:
            return
        clip = min(info['display'], info['remaining'])
        new_id = self.nextQuoteID
        self.nextQuoteID += 1
        q = {
            'type': 'limit',
            'side': book_side,
            'qty': int(clip),
            'price': info['price'],
            'tid': info['tid'],
            'idNum': new_id,
            'timestamp': self.time
        }
        tree = self.bids if book_side == 'bid' else self.asks
        order = tree.insert_order(q)
        info['remaining'] -= clip
        if info['remaining'] > 0:
            self.icebergs[new_id] = info

    # ----- order handlers -----
    def _process_market(self, quote, verbose=False):
        side = quote['side'].lower()
        qtyToTrade = int(quote['qty'])
        trades = []
        if side == 'bid':
            while qtyToTrade > 0:
                best_ask = self.getBestAsk()
                if best_ask is None:
                    break
                lvl = self.asks.priceMap[best_ask]
                qtyToTrade, newTrades = self._process_price_level('ask', lvl, qtyToTrade, quote)
                trades += newTrades
        else:
            while qtyToTrade > 0:
                best_bid = self.getBestBid()
                if best_bid is None:
                    break
                lvl = self.bids.priceMap[best_bid]
                qtyToTrade, newTrades = self._process_price_level('bid', lvl, qtyToTrade, quote)
                trades += newTrades
        return trades

    def _process_limit(self, quote, verbose=False):
        side = quote['side'].lower()
        price = int(quote['price'])
        qtyToTrade = int(quote['qty'])
        tif = quote.get('tif', 'GTC').upper()

        # Post-Only flags (alternative API to tif)
        post_only = bool(quote.get('post_only', False))
        post_only_mode = quote.get('post_only_mode', 'reject')  # 'reject'|'reprice'

        trades = []

        # FOK precheck: reject if not fully fillable
        if tif == 'FOK':
            need = qtyToTrade
            can = self._sum_depth_fok('bid' if side == 'bid' else 'ask', price)
            if need > can:
                return trades, None

        # Post-Only guard: if marketable, either reject or reprice to passive
        if post_only and self._is_marketable(side, price):
            if post_only_mode == 'reject':
                return trades, None
            else:
                price = self._reprice_to_passive(side, price)
                quote['price'] = price
                # still marketable? reject to be safe
                if self._is_marketable(side, price):
                    return trades, None

        # Normal cross before resting (GTC/IOC/FOK)
        if side == 'bid':
            while qtyToTrade > 0:
                best_ask = self.getBestAsk()
                if best_ask is None or best_ask > price:
                    break
                lvl = self.asks.priceMap[best_ask]
                qtyToTrade, newTrades = self._process_price_level('ask', lvl, qtyToTrade, quote)
                trades += newTrades
        else:
            while qtyToTrade > 0:
                best_bid = self.getBestBid()
                if best_bid is None or best_bid < price:
                    break
                lvl = self.bids.priceMap[best_bid]
                qtyToTrade, newTrades = self._process_price_level('bid', lvl, qtyToTrade, quote)
                trades += newTrades

        # IOC leaves nothing
        if tif == 'IOC':
            return trades, None

        # If leftover qty <=0, done
        if qtyToTrade <= 0:
            return trades, None

        # Handle iceberg first-post (only for resting)
        iceberg_total = quote.get('iceberg_total')
        iceberg_display = quote.get('iceberg_display')
        is_iceberg = iceberg_total is not None and iceberg_display is not None

        tree = self.bids if side == 'bid' else self.asks

        if is_iceberg:
            display = int(iceberg_display)
            total = int(iceberg_total)
            clip = min(display, total, qtyToTrade)
            qdict = {
                'idNum': quote['idNum'],
                'tid': quote['tid'],
                'price': price,
                'qty': int(clip),
                'timestamp': quote['timestamp']
            }
            order = tree.insert_order(qdict)
            remaining = total - clip
            if remaining > 0:
                self.icebergs[order.idNum] = {
                    'price': price,
                    'display': display,
                    'remaining': remaining,
                    'tid': quote['tid'],
                }
            return trades, order
        else:
            # simple resting limit
            qdict = {
                'idNum': quote['idNum'],
                'tid': quote['tid'],
                'price': price,
                'qty': int(qtyToTrade),
                'timestamp': quote['timestamp']
            }
            order = tree.insert_order(qdict)
            return trades, order
