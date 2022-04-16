from server.cluster_server import ClusterNodeServer

if __name__ == '__main__':
    master_node = ClusterNodeServer(node_ip="127.0.0.1", node_port=8182)
    master_node.start_service()

