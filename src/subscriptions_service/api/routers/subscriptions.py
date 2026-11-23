from typing import Annotated

from fastapi import APIRouter, Path, Response, status

from subscriptions_service.api.deps import SubscriptionServiceDep
from subscriptions_service.api.schemas import SubscriptionOut, SubscriptionUpsertIn

router = APIRouter(prefix="/subscriptions", tags=["subscriptions"])

SubscriptionId = Annotated[str, Path(min_length=1, max_length=255)]


@router.put(
    "/{subscription_id}",
    responses={status.HTTP_201_CREATED: {"model": SubscriptionOut, "description": "Created"}},
)
async def upsert_subscription(
    subscription_id: SubscriptionId,
    body: SubscriptionUpsertIn,
    service: SubscriptionServiceDep,
    response: Response,
) -> SubscriptionOut:
    """Create or update a subscription and (re)schedule its reminders."""
    result = await service.upsert(
        subscription_id, body.user_id, body.day_count, body.expected_expires_on
    )
    response.status_code = status.HTTP_201_CREATED if result.created else status.HTTP_200_OK
    return SubscriptionOut.model_validate(result.view, from_attributes=True)


@router.get("/{subscription_id}")
async def get_subscription(
    subscription_id: SubscriptionId, service: SubscriptionServiceDep
) -> SubscriptionOut:
    """A subscription with its full notification history."""
    return SubscriptionOut.model_validate(await service.get(subscription_id), from_attributes=True)
