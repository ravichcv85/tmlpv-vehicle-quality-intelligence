"""Foundation Model API client for LLM calls."""
import json
import httpx
from server.config import get_oauth_token, get_workspace_host, SERVING_ENDPOINT


def call_llm(prompt: str, max_tokens: int = 500, temperature: float = 0.3) -> str:
    """Call Foundation Model API and return the text response."""
    host = get_workspace_host()
    token = get_oauth_token()
    url = f"{host}/serving-endpoints/{SERVING_ENDPOINT}/invocations"

    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    with httpx.Client(timeout=60.0) as client:
        response = client.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()

    # Extract text from response
    choices = data.get("choices", [])
    if choices:
        return choices[0].get("message", {}).get("content", "")
    return ""


def classify_complaint(description: str, model: str, severity: str) -> dict:
    """Use LLM to classify a complaint into category/subcategory."""
    prompt = f"""You are an automotive quality analyst for Tata Motors passenger vehicles.

Given this vehicle complaint, classify it into a category and subcategory.

Vehicle Model: {model}
Severity: {severity}
Complaint Description: {description}

Categories to choose from:
- Engine & Powertrain (subcategories: Engine Noise, Oil Leak, Turbo Issue, Starting Problem, Power Loss)
- Electrical System (subcategories: Battery Drain, Wiring Fault, Sensor Malfunction, ECU Error, Alternator Issue)
- Body & Exterior (subcategories: Paint Defect, Rust, Panel Gap, Dent, Scratch)
- Interior & Comfort (subcategories: Seat Defect, AC Malfunction, Noise/Rattle, Dashboard Issue, Upholstery Damage)
- Brakes & Safety (subcategories: Brake Noise, Brake Failure, ABS Issue, Airbag Warning, Seatbelt Problem)
- Suspension & Steering (subcategories: Steering Vibration, Suspension Noise, Alignment Issue, Shock Absorber, Tie Rod)
- Infotainment & Electronics (subcategories: Touchscreen Issue, Bluetooth Problem, Speaker Defect, Navigation Error, Camera Malfunction)
- Tyres & Wheels (subcategories: Premature Wear, Puncture, Wheel Bearing, Alloy Damage, TPMS Error)

Respond ONLY with a JSON object (no markdown, no explanation):
{{"category": "...", "subcategory": "...", "confidence": 0.XX, "reasoning": "brief one-line reason"}}
"""
    raw = call_llm(prompt, max_tokens=200, temperature=0.1)
    # Parse JSON from response
    try:
        # Try to extract JSON from the response
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        return json.loads(raw)
    except (json.JSONDecodeError, IndexError):
        return {
            "category": "Uncategorized",
            "subcategory": "Unknown",
            "confidence": 0.0,
            "reasoning": f"Failed to parse LLM response: {raw[:200]}",
        }


def generate_gap_recommendation(category: str, subcategory: str, gap_data: list[dict]) -> str:
    """Use LLM to generate checklist gap analysis recommendation."""
    gap_summary = "\n".join(
        f"- Complaint: {g.get('category')}/{g.get('subcategory')} "
        f"(count={g.get('complaint_count')}, PDI catch rate={g.get('pdi_catch_rate_pct')}%, "
        f"gap={g.get('gap_classification')})"
        for g in gap_data[:15]
    )

    prompt = f"""You are a senior automotive quality engineer at Tata Motors TMLPV division.

Analyze the following PDI (Pre-Delivery Inspection) checklist gap data for complaint category "{category}" / "{subcategory}":

{gap_summary}

Based on this data:
1. Identify the top gaps where PDI inspection is NOT catching issues that later become customer complaints
2. Recommend specific NEW checklist items that should be added to the PDI process
3. Suggest inspection techniques or tools for each new item
4. Estimate the potential complaint reduction if these items are added

Format your response as a structured analysis with clear sections and bullet points.
Keep it practical and actionable for the quality team.
"""
    return call_llm(prompt, max_tokens=800, temperature=0.4)
