#!/usr/bin/env bash
# Submit one PyFlink job to YARN in application mode, at most once.
#
# Usage: submit_flink_job.sh <job-name> <entry.py> <checkpoints-bucket>
# Runs on the EMR primary node after `aws s3 sync s3://<artifacts>/streaming
# /opt/flink-config-src`. Expects KAFKA_*, ICEBERG_*, BRONZE_SOURCE_SYSTEM and
# *_PARALLELISM in the environment.
#
# - In application mode the PyFlink driver runs inside the YARN JobManager
#   container, which does not inherit this shell's environment. Every variable
#   flink_conf.yaml reads is forwarded explicitly; without that the job falls
#   back to the local defaults (127.0.0.1 Kafka, PLAINTEXT, no warehouse).
# - In application mode the EMR step ends once the job is submitted, so the
#   duplicate check asks YARN, not EMR, whether the job is already running.
# - A restart resumes from the newest retained checkpoint or savepoint of this
#   job (jobs retain checkpoints on cancellation), so `flink stop
#   --savepointPath` followed by a resubmit continues from the savepoint.
#   Delete the job's checkpoint and savepoint prefixes to start fresh after an
#   incompatible job-graph change; the Kafka source then resumes from its
#   committed group offsets.
set -euo pipefail

name="$1"
entry="$2"
bucket="$3"
app="flink-${name}"
src=/opt/flink-config-src
ckpt_root="s3://${bucket}/flink/checkpoints/${name}"

: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
: "${ICEBERG_BRONZE_WAREHOUSE:?ICEBERG_BRONZE_WAREHOUSE is required}"
: "${ICEBERG_SILVER_WAREHOUSE:?ICEBERG_SILVER_WAREHOUSE is required}"
: "${BRONZE_SOURCE_SYSTEM:?BRONZE_SOURCE_SYSTEM is required}"

forwarded=(
  KAFKA_BOOTSTRAP_SERVERS KAFKA_SECURITY_PROTOCOL KAFKA_SASL_MECHANISM
  BRONZE_SOURCE_SYSTEM ICEBERG_BRONZE_WAREHOUSE ICEBERG_SILVER_WAREHOUSE
  INVENTORY_PARALLELISM CLICKSTREAM_PARALLELISM
  INVENTORY_BRONZE_WATERMARK_DELAY_SECONDS INVENTORY_SILVER_WATERMARK_DELAY_SECONDS
  INVENTORY_SILVER_WINDOW
)
env_flags=()
for var in "${forwarded[@]}"; do
  if [ -n "${!var:-}" ]; then
    env_flags+=("-Dcontainerized.master.env.${var}=${!var}"
                "-Dcontainerized.taskmanager.env.${var}=${!var}")
  fi
done

if yarn application -list -appStates SUBMITTED,ACCEPTED,RUNNING 2>/dev/null \
    | awk '{print $2}' | grep -qx "${app}"; then
  echo "${app} is already running on YARN; not submitting a second copy."
  exit 0
fi

savepoint_root="s3://${bucket}/flink/savepoints/${name}"
latest="$( { aws s3 ls --recursive "${ckpt_root}/" || true; aws s3 ls --recursive "${savepoint_root}/" || true; } \
  | awk '$4 ~ /\/_metadata$/ {print $1 " " $2 " " $4}' \
  | sort | tail -n 1 | awk '{print $3}')"

restore=()
if [ -n "${latest}" ]; then
  restore=(-s "s3://${bucket}/${latest%/_metadata}")
  echo "Restoring ${app} from s3://${bucket}/${latest%/_metadata}"
else
  echo "No retained checkpoint for ${app}; starting from committed Kafka offsets."
fi

# Ship the yaml that this run just synced. The JobManager reads whichever
# file sits next to the module; a path on the primary node is not visible
# once YARN places the JobManager on another node.
stage="$(mktemp -d)"
trap 'rm -rf "${stage}"' EXIT
cp -a "${src}/flink_jobs/." "${stage}/"
cp -a "${src}/config/." "${stage}/"

flink run-application -t yarn-application \
  -Dyarn.application.name="${app}" \
  "${env_flags[@]}" \
  -Dstate.checkpoints.dir="${ckpt_root}" \
  -Dstate.savepoints.dir="${savepoint_root}" \
  ${restore[@]+"${restore[@]}"} \
  -pyfs "${stage}" \
  -py "${stage}/${entry}"
