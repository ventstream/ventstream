#!/usr/bin/env bash
# CDC load test against the AWS stack. Runs psql/curl from INSIDE the cluster
# (RDS + OpenSearch are in-VPC, private). Generates N orders, then checks the
# OpenSearch doc count matches. Requires: kubeconfig pointed at the cluster.
set -euo pipefail
NS=ventstream
N="${1:-500000}"

PGURL="postgres://ventstream:$(terraform output -raw db_password)@$(terraform output -raw rds_endpoint):5432/shop"
OS_EP="https://$(terraform output -raw opensearch_endpoint)"
OS_USER=vsadmin
OS_PASS="$(terraform output -raw opensearch_password)"

echo "== generating ${N} orders + items in RDS =="
kubectl -n "$NS" run pgload-$$ --rm -i --restart=Never --image=postgres:16 --env="PGURL=$PGURL" -- \
  psql "$PGURL" -v ON_ERROR_STOP=1 -c "
    INSERT INTO shop.orders (order_id, customer_id, status, total, placed_at)
    SELECT 'ord-a'||g,'cust-00'||(1+(g%5)),'PLACED',(g%900+100)::numeric,now() FROM generate_series(1,${N}) g;
    INSERT INTO shop.order_items (item_id, order_id, sku, qty, price)
    SELECT 'item-a'||g||'-1','ord-a'||g,'SKU-'||(g%50),1+(g%4),(g%90+10)::numeric FROM generate_series(1,${N}) g;
    SELECT 'PG_orders='||count(*) FROM shop.orders;"

echo "== polling OpenSearch doc count (CDC tail) =="
for i in $(seq 1 30); do
  c=$(kubectl -n "$NS" run oscheck-$$-$i --rm -i --restart=Never --image=curlimages/curl -- \
    curl -s -k -u "$OS_USER:$OS_PASS" "$OS_EP/orders/_refresh" >/dev/null 2>&1; \
    kubectl -n "$NS" run oscheck2-$$-$i --rm -i --restart=Never --image=curlimages/curl -- \
    curl -s -k -u "$OS_USER:$OS_PASS" "$OS_EP/orders/_count" 2>/dev/null | grep -o '"count":[0-9]*' | cut -d: -f2)
  echo "  OS_docs=${c:-?}"
  sleep 10
done
echo "Compare against PG_orders above; check engine: kubectl -n $NS logs deploy/vs-cdc"
