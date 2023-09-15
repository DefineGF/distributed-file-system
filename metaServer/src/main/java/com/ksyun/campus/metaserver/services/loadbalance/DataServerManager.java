package com.ksyun.campus.metaserver.services.loadbalance;

import com.ksyun.campus.metaserver.domain.ReplicaData;
import com.ksyun.campus.zookeeper.pojo.DataServerMsg;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 主要完成 DataServer 的负载均衡
 */
@Component
public class DataServerManager {
    private final Set<DataServerMsg> lastWriteServerSet;         // 上次写入的 data-server 副本
    private final Map<String, DataServerMsg> availDataServerMap; // 所有可用的 data-server

    public DataServerManager() {
        lastWriteServerSet = Collections.synchronizedSet(new HashSet<>());
        availDataServerMap = Collections.synchronizedMap(new LinkedHashMap<>());
    }


    /**
     * 添加从 Zookeeper 中获取的可用的 DataServer List
     */
    public void setAvailDataServerList(List<DataServerMsg> ls) {
        availDataServerMap.clear();
        for (DataServerMsg msg : ls) {
            availDataServerMap.put(msg.getNodeId(), msg);
        }
    }

    /**
     * 获取下一次写入的副本信息，满足负载均衡，算法思想：
     * 1. 选择上一次没有选中的：pick1 = all_avail_set - last_write;
     * 2. 如果 pick1 >= count: 从 pick1 中按照 容量利用率 从小到大排序， 选择前 count 个结果；
     * 3. 如果 pick1 <  count: 从 last_write 按照 容量利用率 从小到大排序，
     *    选择前 count - pick1.size() 个结果 => pick2，最终 pick1 + pick2
     */
    public List<DataServerMsg> getNextWritableList(int count, int fileSize) {
        // 获取满足容量 & 不在上次使用集合 & 按照容量使用率排序 的可用列表
        List<DataServerMsg> pickList1 = availDataServerMap.entrySet().stream()
                .filter(entry -> (entry.getValue().getCapacity() - entry.getValue().getUseCapacity()) >= fileSize)
                .filter(entry -> !lastWriteServerSet.contains(entry.getValue()))
                .sorted(Comparator.comparingDouble(entry ->
                        (double) entry.getValue().getUseCapacity() / entry.getValue().getCapacity()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        int need = count - pickList1.size();
        if (need <= 0) {
            return pickList1.stream().limit(count).collect(Collectors.toList());
        } else {
            List<DataServerMsg> last = lastWriteServerSet.stream()
                    .filter(msg -> (msg.getCapacity() - msg.getUseCapacity()) >= fileSize)
                    .sorted(Comparator.comparingDouble(msg -> (double) msg.getUseCapacity() / msg.getCapacity()))
                    .collect(Collectors.toList());
            if (last.size() >= need) {
                // 选择需要个数的
                last = last.stream().limit(need).collect(Collectors.toList());
            }
            pickList1.addAll(last);
            return pickList1;
        }
    }


    /**
     * 获取下一次要读取的副本信息，满足负载均衡，算法思想：
     * - availDataServerList 按照访问先后顺序排列，排列在后面的是最近访问的，前面则是相对很久前才访问的，
     * - [a, d, c, b] 为例：当文件的副本信息存在 b,c,d 副本上时，从前向后遍历列表：
     * - a：没有在可用副本中，下一个；d：在可用副本中，选中，并将 d 放到列表最后，即 [a,c,b,d], 并返回d；
     * - 当再次获取副本时，则从 [a,c,b,d] 中获取，同上，则获取的是 c
     */
    public ReplicaData getNextReadableList(List<ReplicaData> replicaList) {
        ReplicaData ans = null;
        // 从 availDataServerMap 中从前向后遍历，如果在 replicaList 中, 则选中并在 availDataServerMap 调整位置
        Iterator<Map.Entry<String, DataServerMsg>> mapItr = availDataServerMap.entrySet().iterator();
        while (mapItr.hasNext()) {
            DataServerMsg temp = mapItr.next().getValue();
            if (temp != null) {
                Iterator<ReplicaData> lsItr = replicaList.iterator();
                while (lsItr.hasNext()) {
                    ans = lsItr.next();
                    if (temp.getNodeId().equals(ans.getId())) {
                        // 调整 map 中 data-server 顺序
                        mapItr.remove();
                        availDataServerMap.put(temp.getNodeId(), temp);

                        // 调整 list 中 replica-data 顺序 (目标放在首位)
                        lsItr.remove();
                        replicaList.add(0, ans);
                        return ans;
                    }
                }
            }
        }
        return ans;
    }

    /**
     * 通过 data-server write 接口返回的结果 ReplicaData 来更新上次访问的 data-server
     *      主要更新 文件个数
     */
    public void addNewFileUpdate(List<ReplicaData> replicaDataList) {
        if (replicaDataList.size() == 0) {
            return;
        }
        lastWriteServerSet.clear();
        for (ReplicaData replicaData : replicaDataList) {
            DataServerMsg msg = availDataServerMap.get(replicaData.getId());
            lastWriteServerSet.add(msg);
        }
    }

    /**
     * 通过 data-server的写入结果，更新 data:
     *      主要更新容量
     */
    public void writeFileUpdate(List<ReplicaData> replicaDataList, int writeLength) {
        if (replicaDataList == null || replicaDataList.size() == 0) return;
        for (ReplicaData replicaData : replicaDataList) {
            DataServerMsg msg = availDataServerMap.get(replicaData.getId());
            if (msg != null) {
                msg.setUseCapacity(msg.getCapacity() + writeLength);
            }
        }
    }
}
