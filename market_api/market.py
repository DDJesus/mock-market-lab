import queue
import random
import threading

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from uuid import uuid4


CENT = Decimal("0.01")


@dataclass(frozen=True)
class TradeEvent:
    schema_version: str
    event_id: str
    sequence: int
    event_time: str
    symbol: str
    price: Decimal
    volume: int


@dataclass
class MarketState:
    symbol: str
    price: Decimal
    volume: int = 0


class MarketEngine:
    
    def __init__(
        self,
        seed: int | None = None,
        database=None,
        initial_sequence: int = 0,
    ):
        """
        Initialize the MarketEngine.

        Args:
            seed: The random seed for generating trade events.
            database: The database instance for saving trade events.
            initial_sequence: The initial sequence number for trade events.
        """

        self._rng = random.Random(seed)
        self._database = database
        self._sequence = initial_sequence
        self._lock = threading.Lock()
        self._subscribers = []

        # Since this is a mock market, we can define a fixed set of symbols and their initial prices. 
        # In a real market, these would be dynamic. 
        self._market = {
            "CYDE": MarketState("CYDE", Decimal("184.25")),
            "IKOR": MarketState("IKOR", Decimal("327.80")),
            "RELL": MarketState("RELL", Decimal("92.40")),
            "UIM": MarketState("UIM", Decimal("46.75")),
            "RBN": MarketState("RBN", Decimal("241.10")),
        }

    def symbols(self):
        return list(self._market)

    def snapshot(self):
        with self._lock:
            return [
                MarketState(
                    symbol=state.symbol,
                    price=state.price,
                    volume=state.volume,
                )
                for state in self._market.values()
            ]

    def tick(self) -> TradeEvent:
        """
        Generate a new trade event. Used to simulate market activity.

        Returns:
            The generated trade event.
        """
        with self._lock:
            state = self._rng.choice(list(self._market.values()))

            movement = Decimal(
                str(self._rng.uniform(-0.0015, 0.0015))
            )

            new_price = state.price * (Decimal("1") + movement)

            state.price = max(
                CENT,
                new_price.quantize(CENT, rounding=ROUND_HALF_UP),
            )

            trade_volume = self._rng.choice(
                [100, 200, 300, 500, 700, 1000, 1500, 2000, 5000]
            )

            state.volume += trade_volume
            self._sequence += 1

            event = TradeEvent(
                schema_version="market.trade/v1",
                event_id=str(uuid4()),
                sequence=self._sequence,
                event_time=datetime.now(timezone.utc).isoformat(),
                symbol=state.symbol,
                price=state.price,
                volume=trade_volume,
            )
            
            if self._database is not None:
                self._database.save_trade(event)

            # Copy subscribers while protected by the lock.
            subscribers = list(self._subscribers)

        # Publish AFTER releasing the market lock.
        for subscriber in subscribers:
            try:
                subscriber.put_nowait(event)
            except queue.Full:
                pass

        return event

    def run(self, stop_event: threading.Event, interval: float = 0.5) -> None:
        while not stop_event.is_set():
            self.tick()
            stop_event.wait(interval)

    def subscribe(self) -> queue.Queue:
        subscriber = queue.Queue(maxsize=100)

        with self._lock:
            self._subscribers.append(subscriber)

        return subscriber


    def unsubscribe(self, subscriber: queue.Queue) -> None:
        with self._lock:
            if subscriber in self._subscribers:
                self._subscribers.remove(subscriber)