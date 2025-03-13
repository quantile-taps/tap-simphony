"""REST client handling, including SimphonyStream base class."""
from __future__ import annotations
import decimal
import typing as t
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
import requests
from singer_sdk.helpers.types import Context
import secrets
import base64
import hashlib


def generate_code_verifier_and_challenge() -> tuple:
    """Generate a code verifier and challenge for PKCE."""
    # Generate 32 random bytes
    code_string = secrets.token_bytes(32)

    # Encode using URL-safe Base64 without padding
    code_verifier = base64.urlsafe_b64encode(code_string).rstrip(b'=')

    # Convert bytes to string
    code_verifier = code_verifier.decode('utf-8')

    # Hash the verifier using SHA-256
    verifier_bytes = code_verifier.encode('ascii')
    digest = hashlib.sha256(verifier_bytes).digest()

    # Encode the hash using URL-safe Base64 without padding
    code_challenge = base64.urlsafe_b64encode(digest).rstrip(b'=').decode('utf-8')

    return code_verifier, code_challenge

CODE_VERIFIER, CODE_CHALLENGE = generate_code_verifier_and_challenge()



class SimphonyStream(RESTStream):
    """Simphony stream class."""
    records_jsonpath = "$[*]"

    next_page_token_jsonpath = "$.next_page"

    http_method = "POST"

    cookies = None
    code = None
    access_token = None

    authentication_url = "https://mte4-ohra-idm.oracleindustry.com"
    url_base = "https://mte4-ohra.oracleindustry.com/bi/v1/SZH"
    

    def authorize_open_id(self) -> None:
        """
        First step in the authorization process is to get the authorization code.
        """
        url = f"{self.authentication_url}/oidc-provider/v1/oauth2/authorize"

        params = {
            "response_type": "code",
            "client_id": self.config["client_id"],
            "scope": "openid",
            "redirect_uri": "apiaccount://callback",
            "state": "",
            "code_challenge": CODE_CHALLENGE,
            "code_challenge_method": "S256",
        }

        # Make a get request to the authorize endpoint with the params as query string
        response = requests.get(url, params=params, allow_redirects=False)

        # We need to cookies from the response to use in the next request
        cookie = response.headers['Set-Cookie']
        self.cookie = '; '.join(c.split(';')[0] for c in cookie.split(', '))

    def sign_in_api_account(self) -> str:
        url = f"{self.authentication_url}/oidc-provider/v1/oauth2/signin"

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            'Cookie': self.cookie,
        }

        params = {
            "username": self.config["auth_username"],
            "password": self.config["auth_password"],
            "orgname": self.config["organization_identificer"],
            "client_id": self.config["client_id"],
        }

        response = requests.post(url, headers=headers, data=params)
        response_json = response.json()
        
        # Get the code we need to create a token
        self.code = response_json["redirectUrl"].split('code=')[1]

    def create_refresh_token(self) -> str:
        url = f"{self.authentication_url}/oidc-provider/v1/oauth2/token"

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            'Cookie': self.cookie,
        }

        params = {
            "scope": "openid",
            "grant_type": "authorization_code",
            "client_id": self.config["client_id"],
            "code_verifier": CODE_VERIFIER,
            "code": self.code,
            "redirect_uri": "apiaccount://callback",
        }

        response = requests.post(url, headers=headers, data=params)
        response_json = response.json()

        self.access_token = response_json["access_token"]

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        # All the steps required to generate a bearer token
        self.authorize_open_id()
        self.sign_in_api_account()
        self.create_refresh_token()

        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.access_token,
        )

    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict | None:
        return {
            "locRef": self.config["location_reference"],
        }

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )