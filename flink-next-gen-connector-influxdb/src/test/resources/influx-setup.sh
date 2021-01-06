USERNAME=$1
PASSWORD=$2
BUCKET=$3
ORG=$4
DATA_FILE=$5

influx setup \
       -f \
       -u "${USERNAME}" \
       -p "${PASSWORD}" \
       -b "${BUCKET}" \
       -o "${ORG}" \
       -r 0

influx write \
  -b "${BUCKET}" \
  -o "${ORG}" \
  -p ns \
  -f /"${DATA_FILE}"
