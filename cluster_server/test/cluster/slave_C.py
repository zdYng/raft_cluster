from server.cluster_server import ClusterNodeServer

if __name__ == '__main__':
    slave_C_node = ClusterNodeServer(node_ip="127.0.0.1", node_port=9012, is_master_node=False)
    slave_C_node.start_service()
