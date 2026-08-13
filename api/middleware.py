import time
from fastapi import Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from collections import defaultdict


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Simple Sliding Window Rate Limiting Middleware."""

    def __init__(self, app, max_requests: int = 200, window_seconds: int = 60):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_history = defaultdict(list)

    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host if request.client else "127.0.0.1"
        now = time.time()

        # Clean old requests
        self.request_history[client_ip] = [
            t for t in self.request_history[client_ip] if now - t < self.window_seconds
        ]

        if len(self.request_history[client_ip]) >= self.max_requests:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded. Maximum 200 requests per minute allowed."
            )

        self.request_history[client_ip].append(now)
        response = await call_next(request)
        return response
