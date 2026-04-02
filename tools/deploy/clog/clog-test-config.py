load_file('clog.py')

server_list = '127.0.0.1:8045,127.0.0.1:8046,127.0.0.1:8047'
partition_list = '127.0.0.1:8045,127.0.0.1:8046,127.0.0.1:8047'
partition_num = len(partition_list.split(','))
cc1 = make_clog_cluster(hosts=server_list, partitions=partition_list, data_dir='/data')
