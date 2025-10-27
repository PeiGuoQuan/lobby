import pytest
from lobby import OrderBook  # __init__ 暴露了 OrderBook
# 若你的 __init__ 没导出，改成 from lobby.orderbook import OrderBook

def new_book():
    return OrderBook()

def seed_book(book: OrderBook):
    # ask: 101.0 三笔 *5，总量 15；103.0 一笔 *5
    for i in range(3):
        book.processOrder({'type':'limit','side':'ask','qty':5,'price':101.0,'tid':100+i},
                          fromData=False, verbose=False)
    book.processOrder({'type':'limit','side':'ask','qty':5,'price':103.0,'tid':200},
                      fromData=False, verbose=False)
    # bid: 99 / 98 / 97 各 5
    for px, tid in [(99.0,300),(98.0,301),(97.0,302)]:
        book.processOrder({'type':'limit','side':'bid','qty':5,'price':px,'tid':tid},
                          fromData=False, verbose=False)

def test_ioc_partial_does_not_rest():
    book = new_book()
    seed_book(book)
    # 买 IOC 17@102.0：只能吃到 101.0 的 15 手；余量 2 不能挂簿
    trades, order_in_book = book.processOrder(
        {'type':'limit','side':'bid','qty':17,'price':102.0,'tid':999,'tif':'IOC'},
        fromData=False, verbose=False
    )
    # 成交 15（101.0全部吃光）
    assert sum(t['qty'] for t in trades) == 15
    # 101.0 卖盘被清空，最优卖价应变为 103.0
    assert book.getBestAsk() == book.clipPrice(103.0)
    # 关键检查：IOC 余量不挂簿——102.0 买盘不应出现残量
    assert book.getVolumeAtPrice('bid', 102.0) == 0
    # 返回的 in_book 也应为空
    assert order_in_book is None

def test_fok_reject_when_depth_insufficient():
    book = new_book()
    seed_book(book)
    # FOK 买 20@101.0：可成交深度=15，不足则直接拒绝（无成交、簿面不变）
    trades, order_in_book = book.processOrder(
        {'type':'limit','side':'bid','qty':20,'price':101.0,'tid':1000,'tif':'FOK'},
        fromData=False, verbose=False
    )
    assert trades == [] and order_in_book is None
    assert book.getVolumeAtPrice('ask', 101.0) == 15

def test_postonly_reject_if_marketable():
    book = new_book()
    seed_book(book)
    # 现在 seed 后 best_ask = 101.0；挂买单 102.0 且 post_only -> 应被拒绝：不成交、不入簿
    trades, order_in_book = book.processOrder(
        {
            'type': 'limit',
            'side': 'bid',
            'qty': 3,
            'price': 102.0,
            'tid': 500,
            'post_only': True,
            'post_only_mode': 'reject'
        },
        fromData=False, verbose=False
    )
    assert trades == [] and order_in_book is None
    # 102.0 买价位不应出现任何残量
    assert book.getVolumeAtPrice('bid', 102.0) == 0
    # 最优买价/卖价保持不变
    assert book.getBestAsk() == book.clipPrice(101.0)
    assert book.getBestBid() == book.clipPrice(99.0)


def test_postonly_reprice_to_passive():
    book = new_book()
    seed_book(book)
    # 记录下单前的最优买与该价位体量
    best_bid_int = book.getBestBid()
    best_bid_float = best_bid_int / (10 ** book.price_digits)
    vol_before = book.getVolumeAtPrice('bid', best_bid_float)

    # post_only + reprice：挂买 102.0，会穿越 -> 应自动改价到“被动价”并入簿（不立即成交）
    qty = 3
    trades, order_in_book = book.processOrder(
        {
            'type': 'limit',
            'side': 'bid',
            'qty': qty,
            'price': 102.0,
            'tid': 501,
            'post_only': True,
            'post_only_mode': 'reprice'
        },
        fromData=False, verbose=False
    )
    # 不应产生即时成交
    assert trades == []
    assert order_in_book is not None

    # 改价策略（我们在补丁里实现的是“贴同侧最优价”）：
    # 挂到 best_bid 价位，体量应当增加 qty
    vol_after = book.getVolumeAtPrice('bid', best_bid_float)
    assert vol_after == vol_before + qty

    # 仍然没有在 102.0 价位挂残量
    assert book.getVolumeAtPrice('bid', 102.0) == 0
