package com.trilobita.engine.server.masterserver.partitioner;

public class HashPartitioner extends AbstractPartitioner {

    private final int nWorkers;

    public HashPartitioner(int nWorkers){
        this.nWorkers = nWorkers;
    }

    @Override
    public PartitionStrategy getPartitionStrategy() {
        return new PartitionStrategy() {
            @Override
            public int getServerIdByVertexId(int vertexId) {
                int serverId;
                if (vertexId % nWorkers == 0){
                    serverId = nWorkers;
                } else {
                  serverId = vertexId % nWorkers;
                }
                return serverId;
            }
        };
    }
}
