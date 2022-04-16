from server.cluster_server import ClusterNodeServer

if __name__ == '__main__':
    slave_B_node = ClusterNodeServer(node_ip="127.0.0.1", node_port=9192, is_master_node=False)
    slave_B_node.start_service()
