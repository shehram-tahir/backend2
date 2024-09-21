from typing import Dict, Any
from fastapi import HTTPException, status
from all_types.myapi_dtypes import ReqAddPaymentMethod
from all_types.response_dtypes import ResAddPaymentMethod
from storage import load_user_profile, update_user_profile
import uuid

async def add_payment_method(req: ReqAddPaymentMethod) -> Dict[str, Any]:
    # user_data = load_user_profile(req.user_id)
    # if not user_data:
    #     raise HTTPException(
    #         status_code=status.HTTP_404_NOT_FOUND,
    #         detail="User not found"
    #     )

    # # Generate a unique ID for the payment method
    # payment_method_id = str(uuid.uuid4())

    # # Create a new payment method object
    # new_payment_method = {
    #     "id": payment_method_id,
    #     "type": req.payment_type,
    #     "details": req.payment_details
    # }

    # # Add the new payment method to the user's profile
    # if "payment_methods" not in user_data:
    #     user_data["payment_methods"] = []
    # user_data["payment_methods"].append(new_payment_method)

    # # Update the user profile in the database
    # update_user_profile(req.user_id, user_data)

    return "thank you"