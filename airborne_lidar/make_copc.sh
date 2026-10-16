#this will convert all LAS files in the present folder into COPC format and make a footprint in GEOJSON format
#this requires the airborne_lidar env that can be built with airborne_lidar.yml

files=( *.las )
total=${#files[@]}
i=0

for f in "${files[@]}"; do
  i=$((i+1))
  base="${f%.las}"

  echo "[$(date '+%H:%M:%S')] [$i/$total] Converting $f -> ${base}.copc.laz"

  pdal translate "$f" "${base}.copc.laz" \
    --writers.copc.forward=all && \
  echo "[$(date '+%H:%M:%S')] [$i/$total] Building footprint for ${base}.copc.laz" && \
  pdal tindex create \
    --tindex "${base}_footprint.geojson" \
    --filespec "${base}.copc.laz" \
    --ogrdriver GeoJSON && \
  echo "[$(date '+%H:%M:%S')] [$i/$total] Done: $base"
done
