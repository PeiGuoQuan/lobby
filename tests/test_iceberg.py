import pytest
from lobby import OrderBook

def new_book():
    return OrderBook()

def seed_basic(book: OrderBook):
    # 先挂一些基础簿：买端 99/98/97 各 5；方便我们用“市价买”去吃 ask 101
    for px in [99.0, 98.0, 97.0]:
        book.processOrder({'type':'limit','side':'bid','qty':5,'price':px,'tid':1000+int(px)}, False, False)

def head_tid_at_ask(book: OrderBook, price: float):
    p = book.clipPrice(price)
    if book.asks.priceExists(p):
        return book.asks.getPrice(p).getHeadOrder().tid
    return None

def test_iceberg_replenish_and_tail_placement():
    book = new_book()
    seed_basic(book)
    # ask 101：先挂一笔普通 5（tid=1），再挂一笔冰山 total=12 display=5（tid=2），再挂普通 5（tid=3）
    book.processOrder({'type':'limit','side':'ask','qty':5,'price':101.0,'tid':1}, False, False)
    book.processOrder({'type':'limit','side':'ask','qty':12,'price':101.0,'tid':2,
                       'iceberg_total':12,'iceberg_display':5}, False, False)
    book.processOrder({'type':'limit','side':'ask','qty':5,'price':101.0,'tid':3}, False, False)

    # 市价买 5：吃掉 tid=1
    book.processOrder({'type':'market','side':'bid','qty':5,'tid':9}, False, False)
    # 101 的队列顺序：tid=2(首露头5) -> tid=3(5)

    # 市价买 5：吃掉冰山首露头（tid=2），应触发补量，把新的露头放到队尾
    book.processOrder({'type':'market','side':'bid','qty':5,'tid':10}, False, False)
    assert book.getVolumeAtPrice('ask', 101.0) == 10  # 冰山剩余5(以3露头+2隐藏) + 普通5
    # 队首应该是普通单 tid=3（证明冰山“补量入队尾”）
    assert head_tid_at_ask(book, 101.0) == 3



def test_iceberg_drains_to_zero():
    book = new_book()
    seed_basic(book)
    book.processOrder({'type':'limit','side':'ask','qty':8,'price':101.0,'tid':20,
                       'iceberg_total':8,'iceberg_display':3}, False, False)
    book.processOrder({'type':'limit','side':'ask','qty':5,'price':101.0,'tid':21}, False, False)

    # 吃掉首露头3 -> 触发补量，队尾补3；可见 5 + 3 = 8
    book.processOrder({'type':'market','side':'bid','qty':3,'tid':30}, False, False)
    assert book.getVolumeAtPrice('ask', 101.0) == 8

    # 再吃 5：只吃掉普通5，冰山露头3仍在
    book.processOrder({'type':'market','side':'bid','qty':5,'tid':31}, False, False)

    # 一口扫光（露头3 + 再补的2）
    book.processOrder({'type':'market','side':'bid','qty':100,'tid':32}, False, False)
    assert book.getVolumeAtPrice('ask', 101.0) == 0

