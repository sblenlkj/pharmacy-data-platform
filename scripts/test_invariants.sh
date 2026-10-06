#!/bin/sh
set -eu

run_negative() {
  invariant="$1"
  database="$2"
  expected="$3"
  file="$4"
  output_file="$(mktemp)"

  if { printf '\\set VERBOSITY verbose\nBEGIN;\n'; cat "$file"; printf '\nCOMMIT;\n'; } \
      | docker compose exec -T postgres psql -X -v ON_ERROR_STOP=1 -U pharmacy -d "$database" \
      >"$output_file" 2>&1; then
    printf '%s FAIL: completed without an error\n' "$invariant"
    rm -f "$output_file"
    return 1
  fi

  actual="$(sed -E -n 's/^(psql:.*)?ERROR:  ([A-Z0-9]{5}):.*/\2/p' "$output_file" | tail -1)"
  if [ "$actual" != "$expected" ]; then
    printf '%s FAIL: expected %s, got %s\n' "$invariant" "$expected" "${actual:-unknown}"
    cat "$output_file"
    rm -f "$output_file"
    return 1
  fi
  printf '%s PASS SQLSTATE=%s\n' "$invariant" "$actual"
  rm -f "$output_file"
}

run_positive() {
  test_name="$1"
  database="$2"
  file="$3"
  output_file="$(mktemp)"
  { printf 'BEGIN;\n'; cat "$file"; printf '\nROLLBACK;\n'; } \
    | docker compose exec -T postgres psql -X -v ON_ERROR_STOP=1 -At -U pharmacy -d "$database" \
    >"$output_file" 2>&1
  if ! grep -qx 't' "$output_file"; then
    printf '%s FAIL: expected one true result\n' "$test_name"
    cat "$output_file"
    rm -f "$output_file"
    return 1
  fi
  printf '%s PASS ok=true\n' "$test_name"
  rm -f "$output_file"
}

run_negative I1 catalog_service_db 23505 tests/invariants/I01_business_key_unique.sql
run_negative I2 catalog_service_db P0001 tests/invariants/I02_business_key_immutable.sql
run_negative I3 pos_service_db P0001 tests/invariants/I03_receipt_total.sql
run_negative I4 pos_service_db 23514 tests/invariants/I04_receipt_line_arithmetic.sql
run_negative I5 catalog_service_db 23P01 tests/invariants/I05_price_interval_overlap.sql
run_negative I6 catalog_service_db 23505 tests/invariants/I06_single_current_version.sql
run_negative I7 wms_service_db 23514 tests/invariants/I07_stock_non_negative.sql
run_negative I8 pos_service_db P0001 tests/invariants/I08_expired_batch_sale.sql
run_negative I9 wms_service_db 23514 tests/invariants/I09_expired_batch_receipt.sql
run_negative I10 pos_service_db P0001 tests/invariants/I10_rx_without_prescription.sql
run_negative I11 pos_service_db P0001 tests/invariants/I11_refund_exceeds_sale.sql
run_negative I12 pos_service_db P0001 tests/invariants/I12_payment_total.sql
run_positive P01 catalog_service_db tests/invariants/P01_updated_at.sql
run_positive P02 crm_service_db tests/invariants/P02_customer_soft_delete.sql
