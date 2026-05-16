import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.trace import Status, StatusCode
from pydantic import BaseModel, Field

from database import (
    MEMORY_MODE, AsyncSessionLocal, engine,
    get_previous_order_count, get_product, reserve_stock,
)
from telemetry import configure_telemetry

SERVICE_NAME = "inventory-service"

tracer, meter = configure_telemetry(SERVICE_NAME)
logger = logging.getLogger(SERVICE_NAME)

# ── Metricas custom ───────────────────────────────────────────────────────────
inventory_checks = meter.create_counter(
    name="shoptech.inventory.checks",
    description="Total inventory check requests received",
    unit="1",
)
stock_reserved_units = meter.create_counter(
    name="shoptech.inventory.stock_reserved_units",
    description="Total units of stock successfully reserved",
    unit="units",
)
db_query_ms = meter.create_histogram(
    name="shoptech.inventory.db.query_duration_ms",
    description="Latency of individual database queries",
    unit="ms",
)
stock_unavailable = meter.create_counter(
    name="shoptech.inventory.stock_unavailable",
    description="Inventory checks that returned insufficient stock",
    unit="1",
)


class ReserveRequest(BaseModel):
    product_id: str = Field(..., examples=["prod-001"])
    quantity: int = Field(..., gt=0, examples=[2])
    order_id: str = Field(..., examples=["550e8400-e29b-41d4-a716-446655440000"])
    customer_id: str = Field(..., examples=["cust-123"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not MEMORY_MODE and engine is not None:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
        SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)
    mode = "memory" if MEMORY_MODE else "postgresql"
    logger.info("Inventory service started", extra={"db_mode": mode})
    yield
    if not MEMORY_MODE:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
        SQLAlchemyInstrumentor().uninstrument()


app = FastAPI(title="ShopTech - Inventory Service", version="1.0.0", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app)


@app.get("/health", tags=["ops"])
async def health():
    return {"status": "healthy", "service": SERVICE_NAME, "db_mode": "memory" if MEMORY_MODE else "postgresql"}


@app.get("/inventory/{product_id}")
async def check_inventory(
    product_id: str,
    quantity: int = Query(default=1, ge=1),
):
    current_span = trace.get_current_span()
    current_span.set_attribute("inventory.product_id", product_id)
    current_span.set_attribute("inventory.requested_quantity", quantity)

    inventory_checks.add(1, {"product_id": product_id})
    logger.info("Inventory check requested", extra={"product_id": product_id, "quantity": quantity})

    session = AsyncSessionLocal() if not MEMORY_MODE else None

    try:
        # ── Span: consulta producto ──────────────────────────────────────────
        with tracer.start_as_current_span("db.query_product") as span:
            span.set_attribute("db.system", "memory" if MEMORY_MODE else "postgresql")
            span.set_attribute("db.product_id", product_id)
            t0 = time.perf_counter()

            if MEMORY_MODE:
                product = await get_product(product_id)
            else:
                async with session:
                    product = await get_product(product_id, session)

            db_query_ms.record((time.perf_counter() - t0) * 1000, {"operation": "select_product"})

            if product is None:
                _fail(span, "product not found")
                logger.warning("Product not found", extra={"product_id": product_id})
                raise HTTPException(404, f"Product '{product_id}' not found")

            span.set_attribute("db.product_name", product["name"])
            span.set_attribute("db.current_stock", product["stock"])

        # ── Span: historial de ordenes ───────────────────────────────────────
        with tracer.start_as_current_span("db.query_order_history") as span:
            span.set_attribute("db.product_id", product_id)
            t0 = time.perf_counter()

            if MEMORY_MODE:
                previous_orders = await get_previous_order_count(product_id)
            else:
                async with AsyncSessionLocal() as s:
                    previous_orders = await get_previous_order_count(product_id, s)

            db_query_ms.record((time.perf_counter() - t0) * 1000, {"operation": "count_orders"})
            span.set_attribute("db.previous_orders_count", previous_orders)

    finally:
        pass

    available = product["stock"] >= quantity
    current_span.set_attribute("inventory.available", available)
    current_span.set_attribute("inventory.current_stock", product["stock"])
    current_span.set_attribute("inventory.previous_orders", previous_orders)

    if not available:
        stock_unavailable.add(1, {"product_id": product_id})

    logger.info(
        "Inventory check completed",
        extra={
            "product_id": product_id,
            "current_stock": product["stock"],
            "requested_quantity": quantity,
            "available": available,
            "previous_orders": previous_orders,
        },
    )

    return {
        "product_id": product_id,
        "product_name": product["name"],
        "stock": product["stock"],
        "price": product["price"],
        "available": available,
        "previous_orders": previous_orders,
    }


@app.post("/inventory/reserve", status_code=201)
async def reserve_inventory(req: ReserveRequest):
    current_span = trace.get_current_span()
    current_span.set_attribute("reserve.product_id", req.product_id)
    current_span.set_attribute("reserve.quantity", req.quantity)
    current_span.set_attribute("reserve.order_id", req.order_id)

    logger.info("Stock reservation requested", extra={
        "product_id": req.product_id, "quantity": req.quantity, "order_id": req.order_id,
    })

    with tracer.start_as_current_span("db.reserve_stock") as span:
        span.set_attribute("db.product_id", req.product_id)
        span.set_attribute("db.quantity", req.quantity)
        t0 = time.perf_counter()

        if MEMORY_MODE:
            success = await reserve_stock(req.product_id, req.quantity, req.order_id, req.customer_id)
        else:
            async with AsyncSessionLocal() as s:
                success = await reserve_stock(req.product_id, req.quantity, req.order_id, req.customer_id, s)

        db_query_ms.record((time.perf_counter() - t0) * 1000, {"operation": "reserve_stock"})

        if not success:
            _fail(span, "insufficient stock for reservation")
            logger.error("Stock reservation failed", extra={"product_id": req.product_id})
            raise HTTPException(409, "Insufficient stock for reservation")

        span.set_attribute("reserve.success", True)
        stock_reserved_units.add(req.quantity, {"product_id": req.product_id})

    logger.info("Stock reserved successfully", extra={
        "product_id": req.product_id, "quantity": req.quantity, "order_id": req.order_id,
    })
    return {"reserved": True, "product_id": req.product_id, "quantity": req.quantity}


def _fail(span, message: str) -> None:
    span.set_status(Status(StatusCode.ERROR, message))
    span.set_attribute("error.message", message)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
