psql -h 10.114.226.160 --user maps NwDelivery_1711 -c 'copy wlink (mid, mesh, link_id, snode_id, enode_id, link_l) to stdout with csv header' > wlink.csv
psql -h 10.114.226.160 --user maps NwDelivery_1711 -c 'copy wnode (mid, mesh, node_id, edge_mesh, edge_node) to stdout with csv header' > wnode.csv
