from accounts.models import User, UserToken
from allauth.account.adapter import get_adapter
from allauth.account.utils import complete_signup
from allauth.account import app_settings as allauth_settings
from fastapi import HTTPException
from fastapi_auth.utils.token import create_tokens
from django.contrib.auth.hashers import check_password
from fastapi.requests import Request

from allauth.account.models import EmailAddress, EmailConfirmation


class DummyRequest:
    def __init__(self, user):
        self.user = user
        self.session = {}
        self.META = {}

    def build_absolute_uri(self, location=None):
        base_url = "http://localhost:8000"
        if location and "accounts/confirm-email" in location:
            key = location.rstrip("/").split("/")[-1]
            location = f"/verify-email/{key}"
        if not location:
            return base_url
        if location.startswith("http"):
            return location
        return f"{base_url.rstrip('/')}/{location.lstrip('/')}"

    def get_host(self):
        return "localhost:8000"


def trigger_email_verification(user):
    request = DummyRequest(user)
    try:
        email_address = EmailAddress.objects.get(user=user, primary=True)
    except EmailAddress.DoesNotExist:
        raise Exception("No primary email found for user")
    confirmation = EmailConfirmation.create(email_address)
    adapter = get_adapter(user)
    adapter.send_confirmation_mail(request, confirmation, signup=True)
    # email_address.send_confirmation(request)


def confirm_email(user, email):
    email_obj = EmailAddress.objects.get(user=user, email=email)
    email_obj.verified = True
    email_obj.save()


class DummySession(dict):
    def __getitem__(self, key):
        return super().get(key, None)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)


def CreateUser(data):
    if User.objects.filter(email=data.email).exists():
        raise HTTPException(status_code=400, detail="Email already registered")
    user = User.objects.create_user(
        email=data.email,
        password=data.password,
    )
    user_instance = User.objects.get(email=data.email)
    EmailAddress.objects.create(
        user=user_instance, email=user_instance.email, primary=True, verified=False
    )

    trigger_email_verification(user_instance)

    # Complete signup (sets email verification, etc.)
    # TODO do custom email verification system using All auth
    return user


# import jwt
# import datetime
# from fastapi import HTTPException
# from django.contrib.auth.models import User
# from django.conf import settings

# # Secret key from Django settings
# JWT_SECRET = settings.SECRET_KEY
# JWT_ALGORITHM = "HS256"
# ACCESS_TOKEN_LIFETIME = 15  # minutes
# REFRESH_TOKEN_LIFETIME = 7  # days
from django.utils import timezone
from datetime import timedelta

# MAX_ATTEMPTS = 5
# BLOCK_DURATION = timedelta(minutes=15)

from django.core.mail import send_mail
from django.conf import settings


def get_client_ip(request: Request):
    x_forwarded_for = request.headers.get("x-forwarded-for")
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0]
    return request.client.host


def AuthenticateUser(request, data, remember_me):
    if not User.objects.filter(email=data.email).exists():
        raise HTTPException(status_code=400, detail="No record found")
    # Check if blocked

    user = User.objects.get(email=data.email)
    # print(user.is_blocked, user.blocked_until > timezone.now())
    if user.is_blocked and user.blocked_until and user.blocked_until > timezone.now():
        raise HTTPException(
            status_code=403,
            detail=f"Account blocked. Try again at {user.blocked_until}",
        )

    if not check_password(data.password, user.password):
        user.failed_login_attempts += 1
        user.last_failed_login = timezone.now()
        if user.failed_login_attempts >= settings.MAX_FAILED_LOGIN_ATTEMPTS:
            user.is_blocked = True
            user.blocked_until = timezone.now() + settings.BLOCK_DURATION
            # Send email notification
            send_mail(
                subject="Account Blocked",
                message=f"Your account has been blocked due to {settings.MAX_FAILED_LOGIN_ATTEMPTS} failed login attempts. "
                f"You can try logging in again after {settings.BLOCK_DURATION}.",
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=[user.email],
            )

        user.save()
        raise HTTPException(status_code=401, detail="Invalid email or password")
    # For User Login In Multiple Devices
    access_token, refresh_token = create_tokens(user, remember_me)
    token = UserToken.objects.create(
        user=user,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("user-agent", ""),
        access_expires_at=timezone.now() + timedelta(minutes=15),
        refresh_expires_at=timezone.now() + timedelta(days=7),
        access_token=access_token,
        refresh_token=refresh_token,
    )
    # Successful login
    user.failed_login_attempts = 0
    user.is_blocked = False
    user.blocked_until = None
    user.save()

    return {
        "user": {
            "id": user.id,
            "email": user.email,
        },
        settings.ACCESS_TOKEN_NAME: access_token,
        settings.REFRESH_TOKEN_NAME: refresh_token,
    }
