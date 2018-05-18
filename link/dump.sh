psql -h 10.114.226.160 --user maps NwDelivery_1711 -c 'copy link (mid, mesh, link_id, snode_id, enode_id, link_l, max_speed, pass_code, k_control) to stdout with csv header' > link.csv
psql -h 10.114.226.160 --user maps NwDelivery_1711 -c 'copy node (mid, mesh, node_id, edge_mesh, edge_node) to stdout with csv header' > node.csv
