# Mapping of zip codes to human-readable neighborhood names.
# Used on insights pages when the geographic breakdown is by zip code.
# Only needed for regions where city field has a single dominant value.

ZIP_NEIGHBORHOODS = {
    'philadelphia': {
        '19102': 'Center City',
        '19103': 'Center City West / Rittenhouse',
        '19104': 'University City',
        '19106': 'Old City / Society Hill',
        '19107': 'Chinatown / Washington Square',
        '19109': 'Center City East',
        '19111': 'Fox Chase / Burholme',
        '19112': 'Navy Yard',
        '19114': 'Torresdale',
        '19115': 'Bustleton',
        '19116': 'Somerton',
        '19118': 'Chestnut Hill / Mt Airy',
        '19119': 'Mt Airy / Germantown',
        '19120': 'Olney / Logan',
        '19121': 'Brewerytown / Fairmount',
        '19122': 'Northern Liberties / Temple',
        '19123': 'Northern Liberties / Spring Garden',
        '19124': 'Frankford',
        '19125': 'Fishtown / Kensington',
        '19126': 'Oak Lane / Cedarbrook',
        '19127': 'Manayunk / Roxborough',
        '19128': 'Roxborough / Andorra',
        '19129': 'East Falls',
        '19130': 'Fairmount / Spring Garden',
        '19131': 'Overbrook',
        '19132': 'Strawberry Mansion / Nicetown',
        '19133': 'North Philadelphia',
        '19134': 'Port Richmond / Kensington',
        '19135': 'Mayfair / Holmesburg',
        '19136': 'Holmesburg / Pennypack',
        '19137': 'Bridesburg / Tacony',
        '19138': 'Germantown / Mt Airy',
        '19139': 'Cobbs Creek',
        '19140': 'North Philadelphia / Hunting Park',
        '19141': 'Fern Rock / Ogontz',
        '19142': 'Southwest Philadelphia',
        '19143': 'Southwest Philadelphia / Kingsessing',
        '19144': 'Germantown',
        '19145': 'South Philadelphia / Girard Estates',
        '19146': 'Graduate Hospital / Point Breeze',
        '19147': 'South Philadelphia / Bella Vista',
        '19148': 'South Philadelphia / Passyunk',
        '19149': 'Mayfair / Oxford Circle',
        '19150': 'Cedarbrook / Lawndale',
        '19151': 'Overbrook / Wynnefield',
        '19152': 'Bustleton / Somerton',
        '19153': 'Eastwick / Airport',
        '19154': 'Torresdale / Byberry',
    },
}


def get_neighborhood_name(region: str, zip_code: str) -> str:
    """Return a neighborhood name for a zip code, or the zip code itself as fallback."""
    region_map = ZIP_NEIGHBORHOODS.get(region, {})
    return region_map.get(str(zip_code), str(zip_code))
