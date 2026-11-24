import uuid
from typing import Annotated

from fastapi import APIRouter, Query, Response, status

from subscriptions_service.api.deps import PaymentServiceDep
from subscriptions_service.api.schemas import PaymentIn, PaymentOut, PaymentStatusOut
from subscriptions_service.config import MAX_TEXT_ID_LENGTH, NO_NUL_PATTERN
from subscriptions_service.services.dto import RecordPaymentCommand

router = APIRouter(prefix="/payments", tags=["payments"])


@router.post(
    "",
    responses={status.HTTP_201_CREATED: {"model": PaymentOut, "description": "Created"}},
)
async def record_payment(
    body: PaymentIn, service: PaymentServiceDep, response: Response
) -> PaymentOut:
    """Record a payment; idempotent on `provider_payment` (identical retry → 200)."""
    result = await service.record(
        RecordPaymentCommand(
            subscription_id=body.subscription_id,
            provider_payment=body.provider_payment,
            amount=body.amount,
            currency=body.currency,
            status=body.status,
        )
    )
    response.status_code = status.HTTP_201_CREATED if result.created else status.HTTP_200_OK
    return PaymentOut.model_validate(result.view, from_attributes=True)


@router.get("/{payment_id}")
async def get_payment(payment_id: uuid.UUID, service: PaymentServiceDep) -> PaymentStatusOut:
    """Payment status, event delivery state and result notification."""
    return PaymentStatusOut.from_view(await service.get_status(payment_id))


@router.get("")
async def get_payment_by_provider(
    provider_payment: Annotated[
        str, Query(min_length=1, max_length=MAX_TEXT_ID_LENGTH, pattern=NO_NUL_PATTERN)
    ],
    service: PaymentServiceDep,
) -> PaymentStatusOut:
    """Same as `GET /payments/{payment_id}`, looked up by the provider's payment id."""
    return PaymentStatusOut.from_view(await service.get_status_by_provider(provider_payment))
