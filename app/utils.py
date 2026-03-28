"""Shared utility helpers."""

REGION_DISPLAY_NAMES = {
    'nyc': 'NYC',
}


def get_region_display(region: str) -> str:
    """Return a human-readable display name for a region slug."""
    return REGION_DISPLAY_NAMES.get(region, region.replace('-', ' ').title())
