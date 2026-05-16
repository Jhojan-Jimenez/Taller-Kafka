import logging
import os
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, status
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.trace import Status, StatusCode
from pydantic import BaseModel, Field

from telemetry import configure_telemetry

SERVICE_NAME = "order-service"
INVENTORY_URL = os.getenv("INVENTORY_SERVICE_URL", "http://inventory-service:8001")

tracer, meter = configure_telemetry(SERVICE_NAME)
logger = logging.getLogger(SERVICE_NAME)

# ── Custom metrics ───────────────────────────────────────────────────────────
orders_created = meter.create_counter(
    name="shoptech.orders.created",
    description="Total orders created successfully",
    unit="1",
)
orders_failed = meter.create_counter(
    name="shoptech.orders.failed",
    description="Total orders that failed (validation, stock, or system errors)",
    unit="1",
)
order_value_usd = meter.create_histogram(
    name="shoptech.order.value_usd",
    description="Distribution of order total values in USD",
    unit="USD",
)
inventory_call_ms = meter.create_histogram(
    name="shoptech.inventory.call.duration_ms",
    description="End-to-end latency of calls to inventory-service",
    unit="ms",
)


# ── Request / Response models ────────────────────────────────────────────────
class OrderRequest(BaseModel):
    product_id: str = Field(..., examples=["prod-001"])
    quantity: int = Field(..., gt=0, examples=[2])
    customer_id: str = Field(..., examples=["cust-123"])


class OrderResponse(BaseModel):
    order_id: str
    product_id: str
    customer_id: str
    quantity: int
    total_price: float
    status: str


# ── App lifecycle ────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Auto-instrument httpx: propagates W3C TraceContext headers on every request
    HTTPXClientInstrumentor().instrument()
    yield
    HTTPXClientInstrumentor().uninstrument()


app = FastAPI(title="ShopTech — Order Service", version="1.0.0", lifespan=lifespan)

# Auto-instrument FastAPI: creates spans for each HTTP route automatically
FastAPIInstrumentor.instrument_app(app)


# ── Endpoints ────────────────────────────────────────────────────────────────
@app.get("/health", tags=["ops"])
async def health():
    return {"status": "healthy", "service": SERVICE_NAME}


@app.post("/orders", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(order: OrderRequest):
    order_id = str(uuid.uuid4())

    # Enrich the auto-created span (from FastAPIInstrumentor) with business attributes
    current_span = trace.get_current_span()
    current_span.set_attribute("order.id", order_id)
    current_span.set_attribute("order.product_id", order.product_id)
    current_span.set_attribute("order.customer_id", order.customer_id)
    current_span.set_attribute("order.quantity", order.quantity)

    logger.info(
        "Order request received",
        extra={
            "order_id": order_id,
            "product_id": order.product_id,
            "customer_id": order.customer_id,
            "quantity": order.quantity,
        },
    )

    # ── Manual span: business validation ────────────────────────────
    with tracer.start_as_current_span("validate_order") as span:
        span.set_attribute("order.product_id", order.product_id)
        span.set_attribute("order.quantity", order.quantity)

        if not order.product_id.strip():
            _fail(span, "empty product_id")
            orders_failed.add(1, {"reason": "invalid_product_id"})
            raise HTTPException(400, "product_id cannot be empty")

        if order.quantity > 1000:
            _fail(span, "quantity exceeds limit")
            orders_failed.add(1, {"reason": "quantity_exceeded"})
            raise HTTPException(400, "Maximum quantity per order is 1000")

        span.set_attribute("validation.result", "passed")
        logger.info("Order validation passed", extra={"order_id": order_id})

    # ── Manual span: inventory check (calls yyy-service) ────────────
    inventory_data: dict = {}
    with tracer.start_as_current_span("check_inventory") as span:
        span.set_attribute("inventory.product_id", order.product_id)
        span.set_attribute("inventory.requested_quantity", order.quantity)
        t0 = time.perf_counter()

        try:
            # HTTPXClientInstrumentor propagates TraceContext here automatically
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(
                    f"{INVENTORY_URL}/inventory/{order.product_id}",
                    params={"quantity": order.quantity},
                )
                resp.raise_for_status()
                inventory_data = resp.json()

        except httpx.HTTPStatusError as exc:
            _fail(span, f"inventory HTTP {exc.response.status_code}")
            span.record_exception(exc)
            orders_failed.add(1, {"reason": "inventory_http_error"})
            logger.error(
                "Inventory service returned error",
                extra={
                    "order_id": order_id,
                    "http_status": exc.response.status_code,
                    "product_id": order.product_id,
                },
            )
            raise HTTPException(503, "Inventory service error")

        except (httpx.ConnectError, httpx.TimeoutException) as exc:
            _fail(span, "inventory unreachable")
            span.record_exception(exc)
            orders_failed.add(1, {"reason": "inventory_unavailable"})
            logger.error(
                "Inventory service unreachable",
                extra={"order_id": order_id, "error": str(exc)},
            )
            raise HTTPException(503, "Inventory service unavailable")

        finally:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            inventory_call_ms.record(elapsed_ms, {"product_id": order.product_id})

        if not inventory_data.get("available"):
            span.set_attribute("inventory.available", False)
            span.set_attribute("inventory.current_stock", inventory_data.get("stock", 0))
            _fail(span, "insufficient stock")
            orders_failed.add(1, {"reason": "insufficient_stock"})
            logger.warning(
                "Order rejected: insufficient stock",
                extra={
                    "order_id": order_id,
                    "product_id": order.product_id,
                    "requested": order.quantity,
                    "available_stock": inventory_data.get("stock", 0),
                },
            )
            raise HTTPException(409, "Insufficient stock")

        span.set_attribute("inventory.available", True)
        span.set_attribute("inventory.current_stock", inventory_data.get("stock", 0))

    # ── Manual span: persist / confirm the order ─────────────────────
    with tracer.start_as_current_span("create_order_record") as span:
        unit_price: float = inventory_data.get("price", 0.0)
        total: float = round(unit_price * order.quantity, 2)

        span.set_attribute("order.unit_price_usd", unit_price)
        span.set_attribute("order.total_price_usd", total)
        span.set_attribute("order.status", "confirmed")

        # Custom metrics: business KPIs visible in New Relic dashboards
        orders_created.add(1, {"product_id": order.product_id})
        order_value_usd.record(total, {"product_id": order.product_id})

        logger.info(
            "Order confirmed",
            extra={
                "order_id": order_id,
                "product_id": order.product_id,
                "total_price_usd": total,
                "status": "confirmed",
            },
        )

    return OrderResponse(
        order_id=order_id,
        product_id=order.product_id,
        customer_id=order.customer_id,
        quantity=order.quantity,
        total_price=total,
        status="confirmed",
    )


def _fail(span, message: str) -> None:
    span.set_status(Status(StatusCode.ERROR, message))
    span.set_attribute("error.message", message)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
