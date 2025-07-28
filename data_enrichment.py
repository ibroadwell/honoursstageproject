import MappingJSONsFromDB as Map
import FoliumMapGenFromJSON as HTMLMap
import StopsEnrichmentPostCode as PCEnrich
import StopsEnrichmentOAs as OAEnrich


Map.GenerateMappingJSONs()
HTMLMap.GenerateHTMLMaps()
PCEnrich.GenerateStopsPostcode()
OAEnrich.GenerateOAs()

