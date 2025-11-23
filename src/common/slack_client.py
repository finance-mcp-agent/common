import hmac
import hashlib
import json
import logging
import time
from typing import Optional, Dict, Any

import httpx

logger = logging.getLogger(__name__)


class SlackClient:
    """
    Minimal Slack Web API + signing verification helper.

    - send_approval_message: send Block Kit card with Approve/Reject/Snooze
    - verify_signature: validate Slack interactive request signatures
    """

    def __init__(
        self,
        bot_token: str,
        signing_secret: Optional[str] = None,
        default_channel: Optional[str] = None,
    ) -> None:
        if not bot_token:
            raise ValueError("Slack bot token is required")

        self.bot_token = bot_token
        self.signing_secret = signing_secret
        self.default_channel = default_channel

        self._client = httpx.Client(
            base_url="https://slack.com/api",
            timeout=5.0,
        )

    # ---------- Outbound messages ----------

    def send_approval_message(
        self,
        channel: Optional[str],
        decision: Dict[str, Any],
        callback_base_url: str,
    ) -> None:
        """
        Send a Block Kit message for an impact-model proposed_decision.

        decision is expected to contain at least:
        - decision_id
        - signal_id
        - ticker
        - direction
        - confidence
        - sentiment
        """
        channel_id = channel or self.default_channel
        if not channel_id:
            raise ValueError("Slack channel not provided and no default_channel set")

        ticker = decision.get("ticker")
        direction = decision.get("direction")
        confidence = decision.get("confidence")
        sentiment = decision.get("sentiment")
        decision_id = decision.get("decision_id")
        signal_id = decision.get("signal_id")

        title = f"{ticker} — {direction} (conf={confidence:.2f})"
        subtitle = f"Sentiment={sentiment}, signal_id={signal_id}"

        # Value stored in each button so we can reconstruct which decision was clicked
        value_payload = json.dumps(
            {
                "decision_id": decision_id,
                "signal_id": signal_id,
                "ticker": ticker,
            }
        )

        # This callback_id helps route inside /slack/actions
        callback_id = "trade_approval"

        blocks = [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{title}*\n{subtitle}"},
            },
            {
                "type": "actions",
                "block_id": "approval_actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "✅ Approve"},
                        "style": "primary",
                        "value": value_payload,
                        "action_id": "approve",
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "❌ Reject"},
                        "style": "danger",
                        "value": value_payload,
                        "action_id": "reject",
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "🕒 Snooze"},
                        "value": value_payload,
                        "action_id": "snooze",
                    },
                ],
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Callback: {callback_base_url}/slack/actions",
                    }
                ],
            },
        ]

        payload = {
            "channel": channel_id,
            "text": title,  # fallback
            "blocks": blocks,
        }

        resp = self._client.post(
            "/chat.postMessage",
            headers={"Authorization": f"Bearer {self.bot_token}"},
            json=payload,
        )
        if not resp.is_success:
            logger.error("Slack postMessage failed: %s", resp.text)
            resp.raise_for_status()

    # ---------- Inbound verification ----------

    def verify_signature(
        self,
        timestamp: str,
        signature: str,
        body: bytes,
        tolerance_seconds: int = 300,
    ) -> bool:
        """
        Verify Slack request signature.

        Slack docs:
        v0=<hex> where hex = HMAC-SHA256(signing_secret,
                                         "v0:" + timestamp + ":" + body)
        """
        if not self.signing_secret:
            # If not set, we just accept (easier for local dev)
            logger.warning("No Slack signing secret configured; skipping verification")
            return True

        if abs(time.time() - int(timestamp)) > tolerance_seconds:
            logger.warning("Slack request timestamp outside tolerance")
            return False

        basestring = f"v0:{timestamp}:{body.decode('utf-8')}".encode("utf-8")
        computed = hmac.new(
            self.signing_secret.encode("utf-8"),
            basestring,
            hashlib.sha256,
        ).hexdigest()
        expected = f"v0={computed}"

        # Constant-time compare
        if not hmac.compare_digest(expected, signature):
            logger.warning("Slack signature mismatch")
            return False

        return True