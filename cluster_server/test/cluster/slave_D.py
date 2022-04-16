from server.cluster_server import ClusterNodeServer

if __name__ == '__main__':
    slave_D_node = ClusterNodeServer(node_ip="127.0.0.1", node_port=9087, is_master_node=False)
    slave_D_node.start_service()
