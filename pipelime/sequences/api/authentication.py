from typing import Callable, Dict, Optional
from fastapi import HTTPException
from fastapi.openapi.models import OAuthFlows as OAuthFlowsModel
from fastapi.security.oauth2 import OAuth2
from fastapi.security.utils import get_authorization_scheme_param
from starlette.requests import Request
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN


class CustomAPIAuthentication(OAuth2):
    def __init__(
        self,
        token_callback: Callable[[str], bool],
        tokenUrl: str = "token",
        scheme_name: Optional[str] = None,
        scopes: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
        auto_error: bool = True,
    ):
        """This is a Custom authentication class passed as Depends into the API endpoints.
        Classical authentication Depends just analyse the Authorization header and return the token.
        This class, instead, is used to verify the token by means of a callback function
        provided by the user. In this way the user can implement a custom authentication
        scheme external to the API.

        :param token_callback: A callback function that takes a token and returns a boolean
        :type token_callback: Callable[[str], bool]
        :param tokenUrl: the default token key , defaults to "token"
        :type tokenUrl: str, optional
        :param scheme_name: authentication scheme name, defaults to None
        :type scheme_name: Optional[str], optional
        :param scopes: authentication scopes, defaults to None
        :type scopes: Optional[Dict[str, str]], optional
        :param description: description , defaults to None
        :type description: Optional[str], optional
        :param auto_error: TRUE to auto-raise authentication error defaults to True
        :type auto_error: bool, optional
        """
        if not scopes:
            scopes = {}
        flows = OAuthFlowsModel(password={"tokenUrl": tokenUrl, "scopes": scopes})
        self._token_callback = token_callback
        self._token_key = tokenUrl
        super().__init__(
            flows=flows,
            scheme_name=scheme_name,
            description=description,
            auto_error=auto_error,
        )

    async def __call__(self, request: Request) -> Optional[str]:

        # Get the token from the Authorization header and check presence
        # If token not present the authentication fails automatically
        authorization: str = request.headers.get("Authorization")
        scheme, token = get_authorization_scheme_param(authorization)
        if not authorization or scheme.lower() != "bearer":
            if self._token_key in request.query_params:
                token = request.query_params[self._token_key]
            else:
                if self.auto_error:
                    raise HTTPException(
                        status_code=HTTP_401_UNAUTHORIZED,
                        detail="Not authenticated",
                        headers={"WWW-Authenticate": "Bearer"},
                    )
                else:
                    return None

        # If the token is present:
        # Verify the token by means of external callback function provided by the user
        if not self._token_callback(token):
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN,
                detail="Invalid Token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return token
