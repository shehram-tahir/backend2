from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from all_types.myapi_dtypes import ReqCreateUserProfile, ReqUserLogin, ReqUserProfile
from storage import USERS_INFO_PATH, load_user_profile, update_user_profile
from jose import jwt, JWTError
import requests
import os
import json
import logging
import os
import uuid
from firebase_admin import auth
import firebase_admin
from firebase_admin import credentials
from config_factory import get_conf

# SECRET_KEY = os.getenv(
#     "SECRET_KEY", "your-secret-key"
# )  # Use environment variable in production
# ALGORITHM = "HS256"
# ACCESS_TOKEN_EXPIRE_MINUTES = 30

# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

CONF = get_conf()

cred = credentials.Certificate('secrets/secret_fir-locator-35839-firebase-adminsdk-yb6f6-a5b81519d9.json')
default_app = firebase_admin.initialize_app(cred)


# async def create_user_profile(req: ReqCreateUserProfile) -> Dict[str, str]:
#     try:
#         if is_username_or_email_taken(req.username, req.email):
#             raise HTTPException(
#                 status_code=status.HTTP_400_BAD_REQUEST,
#                 detail="Username or email already taken",
#             )
#         user_id = generate_user_id()
#         hashed_password = get_password_hash(req.password)

#         user_data = {
#             "user_id": user_id,
#             "username": req.username,
#             "email": req.email,
#             "prdcer": {"prdcer_lyrs": {}, "prdcer_ctlgs": {}},
#         }

#         update_user_profile(user_id, user_data)
#         users_info = load_users_info()
#         users_info["users"].append(
#             {
#                 "user_id": user_id,
#                 "username": req.username,
#                 "email": req.email,
#                 "hashed_password": hashed_password,
#             }
#         )
#         save_users_info(users_info)

#         return {"user_id": user_id, "message": "User profile created successfully"}
#     except Exception as e:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Error creating user profile: {str(e)}",
#         ) from e


async def create_user_profile(req: ReqCreateUserProfile) -> Dict[str, str]:
    try:
        # Create user in Firebase
        user = auth.create_user(
            email=req.email,
            password=req.password,
            display_name=req.username
        )

        user_data = {
            "user_id": user.uid,
            "username": req.username,
            "email": req.email,
            "prdcer": {"prdcer_lyrs": {}, "prdcer_ctlgs": {}},
        }

        # Save additional user data to your database
        update_user_profile(user.uid, user_data)

        return {"user_id": user.uid, "message": "User profile created successfully"}
    except auth.EmailAlreadyExistsError as emialerrror:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already taken",
        ) from emialerrror
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating user profile: {str(e)}",
        ) from e


async def login_user(req: ReqUserLogin) -> Dict[str, str]:
    try:
        url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={CONF.firebase_api_key}"
        payload = {
            "email": req.email,
            "password": req.password,
            "returnSecureToken": True
        }
        response = requests.post(url, json=payload, timeout=120)
        response.raise_for_status()
        data = response.json()


        return {
            "access_token": data["idToken"],
            "token_type": "bearer",
            "expires_in": data["expiresIn"]
        }
    except auth.UserNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )from e
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error during login: {str(e)}",
        ) from e


def my_verify_id_token(token: str = Depends(oauth2_scheme)):
    try:
        return auth.verify_id_token(token)
    except auth.InvalidIdTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token",
            headers={"WWW-Authenticate": "Bearer"},
        )from e
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token validation error: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )from e





# def verify_password(plain_password, hashed_password):
#     return pwd_context.verify(plain_password, hashed_password)


# def get_password_hash(password):
#     return pwd_context.hash(password)


# def get_user_by_username(username: str) -> Optional[Dict]:
#     users_info = load_users_info()
#     for user in users_info["users"]:
#         if user["username"] == username:
#             return user
#     return None


# def authenticate_user(username: str, password: str):
#     user = get_user_by_username(username)
#     if not user or not verify_password(password, user["hashed_password"]):
#         return False
#     return user


# def create_access_token(data: dict, expires_delta: timedelta | None = None):
#     access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
#     to_encode = data.copy()
#     expire = datetime.utcnow() + (access_token_expires or timedelta(minutes=15))
#     to_encode.update({"exp": expire})
#     return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError as jwte:
        raise (credentials_exception) from jwte
    user = get_user_by_username(username)
    if user is None:
        raise credentials_exception
    return user


async def get_user_account(req: ReqUserProfile) -> Dict[str, Any]:
    try:
        user_data = load_user_profile(req.user_id)
        if not user_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
            )

        return {
            "user_id": user_data["user_id"],
            "username": user_data["username"],
            "email": user_data["email"],
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching user profile: {str(e)}",
        )from e


# def decode_access_token(token: str):
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         return payload
#     except JWTError as jwte:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Could not validate credentials",
#             headers={"WWW-Authenticate": "Bearer"},
#         ) from jwte


# def load_users_info() -> Dict:
#     try:
#         if os.path.exists(USERS_INFO_PATH):
#             with open(USERS_INFO_PATH, "r") as f:
#                 return json.load(f)
#         return {"users": []}
#     except json.JSONDecodeError as jsondecoder:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail="Error parsing users info file",
#         ) from jsondecoder


# def save_users_info(users_info: Dict):
#     try:
#         with open(USERS_INFO_PATH, "w") as f:
#             json.dump(users_info, f, indent=2)
#     except IOError as ioe:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail="Error saving users info",
#         ) from ioe


# def is_username_or_email_taken(username: str, email: str) -> bool:
#     users_info = load_users_info()
#     for user in users_info["users"]:
#         if user["username"] == username or user["email"] == email:
#             return True
#     return False


# def generate_user_id() -> str:
#     file_path = "Backend/users_info.json"

#     try:
#         with open(file_path, "r") as file:
#             data = json.load(file)
#             existing_ids = set(user["user_id"] for user in data.get("users", []))
#     except FileNotFoundError:
#         existing_ids = set()
#     except json.JSONDecodeError as jsondecoder:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail="Error parsing users info file",
#         ) from jsondecoder

#     while True:
#         new_id = str(uuid.uuid4())
#         if new_id not in existing_ids:
#             return new_id

