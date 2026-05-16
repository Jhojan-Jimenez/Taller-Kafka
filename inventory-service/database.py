import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://shoptech:shoptech@postgres:5432/shoptech",
)
# Modo memoria: no requiere PostgreSQL. Activo cuando DATABASE_URL=memory://
MEMORY_MODE = DATABASE_URL.startswith("memory://")

# ── Datos en memoria (seed identico a init.sql) ───────────────────────────────
_PRODUCTS: dict[str, dict] = {
    "prod-001": {"id": "prod-001", "name": 'Laptop Pro 15"',      "stock": 50,  "price": 1299.99},
    "prod-002": {"id": "prod-002", "name": "Wireless Headphones", "stock": 200, "price": 89.99},
    "prod-003": {"id": "prod-003", "name": "Mechanical Keyboard", "stock": 150, "price": 129.99},
    "prod-004": {"id": "prod-004", "name": "USB-C Hub 7-port",    "stock": 300, "price": 49.99},
    "prod-005": {"id": "prod-005", "name": 'Monitor 4K 27"',      "stock": 30,  "price": 599.99},
}
_ORDERS: list[dict] = [
    {"id": "ord-hist-001", "product_id": "prod-001", "customer_id": "cust-001", "quantity": 1, "status": "confirmed"},
    {"id": "ord-hist-002", "product_id": "prod-001", "customer_id": "cust-002", "quantity": 2, "status": "confirmed"},
    {"id": "ord-hist-003", "product_id": "prod-002", "customer_id": "cust-001", "quantity": 3, "status": "confirmed"},
    {"id": "ord-hist-004", "product_id": "prod-003", "customer_id": "cust-003", "quantity": 1, "status": "confirmed"},
]

# ── API comun (session=None en modo memoria) ──────────────────────────────────

async def get_product(product_id: str, session=None) -> dict | None:
    if MEMORY_MODE:
        p = _PRODUCTS.get(product_id)
        return dict(p) if p else None
    result = await session.execute(
        __import__("sqlalchemy").text("SELECT id, name, stock, price FROM products WHERE id = :id"),
        {"id": product_id},
    )
    row = result.fetchone()
    return {"id": row[0], "name": row[1], "stock": row[2], "price": float(row[3])} if row else None


async def get_previous_order_count(product_id: str, session=None) -> int:
    if MEMORY_MODE:
        return sum(1 for o in _ORDERS if o["product_id"] == product_id and o["status"] == "confirmed")
    result = await session.execute(
        __import__("sqlalchemy").text(
            "SELECT COUNT(*) FROM orders WHERE product_id = :pid AND status = 'confirmed'"
        ),
        {"pid": product_id},
    )
    return result.scalar() or 0


async def reserve_stock(
    product_id: str, quantity: int, order_id: str, customer_id: str, session=None
) -> bool:
    if MEMORY_MODE:
        p = _PRODUCTS.get(product_id)
        if p is None or p["stock"] < quantity:
            return False
        p["stock"] -= quantity
        _ORDERS.append({
            "id": order_id, "product_id": product_id,
            "customer_id": customer_id, "quantity": quantity, "status": "confirmed",
        })
        return True

    from sqlalchemy import text
    updated = await session.execute(
        text("UPDATE products SET stock = stock - :qty WHERE id = :id AND stock >= :qty RETURNING id"),
        {"qty": quantity, "id": product_id},
    )
    if updated.fetchone() is None:
        await session.rollback()
        return False
    await session.execute(
        text("INSERT INTO orders (id, product_id, customer_id, quantity, status) VALUES (:oid, :pid, :cid, :qty, 'confirmed')"),
        {"oid": order_id, "pid": product_id, "cid": customer_id, "qty": quantity},
    )
    await session.commit()
    return True


# ── Session factory (solo se usa en modo PostgreSQL) ─────────────────────────
def _make_session_factory():
    if MEMORY_MODE:
        return None
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
    return engine, sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


if not MEMORY_MODE:
    engine, AsyncSessionLocal = _make_session_factory()
else:
    engine = None
    AsyncSessionLocal = None
