import stops_enrichment_postcode as SEP
import stops_enrichment_oas as SEO

def enriched_record_from_lon_lat(stop_lat, stop_lon):
    stop_enriched = {}
    stop_enriched["stop_lat"] = stop_lat
    stop_enriched["stop_lon"] = stop_lon
    stop_enriched["postcode"] = SEP.reverse_geocode_postcode(stop_lat, stop_lon)
    oa21cd, lsoa21cd, lsoa21nm = SEO.get_oa_lsoa_details(stop_enriched["postcode"])
    stop_enriched["oa21cd"] = oa21cd
    stop_enriched["lsoa21cd"] = lsoa21cd
    stop_enriched["lsoa21nm"] = lsoa21nm

