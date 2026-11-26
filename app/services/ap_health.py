"""Consistent AP operational state from ingestion and radio evidence."""


def assess_ap(heartbeat_ok, heartbeat_age_s, radio_ages_s, reasons=(), freshness_s=3):
    heartbeat_fresh = heartbeat_age_s is not None and 0 <= heartbeat_age_s <= freshness_s
    fresh_radios = [age for age in radio_ages_s if age is not None and 0 <= age <= freshness_s]
    online = heartbeat_fresh and heartbeat_ok is True
    if not heartbeat_fresh:
        state = {'label': 'Unknown', 'tone': 'neutral'}
    elif heartbeat_ok is not True:
        state = {'label': 'Offline', 'tone': 'crit'}
    elif not fresh_radios or len(fresh_radios) != len(radio_ages_s):
        state = {'label': 'Stale', 'tone': 'neutral'}
    elif reasons:
        state = {'label': 'Degraded', 'tone': 'warn', 'reason': '; '.join(reasons)}
    else:
        state = {'label': 'Healthy', 'tone': 'good'}
    return online, state
