package com.pk.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.google.common.base.CaseFormat;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CanalClient {
    public static void main(String[] args) throws Exception {
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop000", 11111), "example", null, null);
        while(true) {
            connector.connect();
            connector.subscribe("canal.*");
            Message message = connector.get(1000);
            List<CanalEntry.Entry> entries = message.getEntries();
            if(entries.size() > 0) {
                for (CanalEntry.Entry entry : entries) {
                    // 获取表名
                    String tableName = entry.getHeader().getTableName();
                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                    CanalEntry.EventType eventType = rowChange.getEventType();
                    /*
                     * type: insert  update delete
                     * 你仅需要你所关心的类型就可以的
                     *
                     */
                    if(eventType == CanalEntry.EventType.INSERT) {
                        for (CanalEntry.RowData rowData : rowDatasList) {
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                            Map<String,String> map = new HashMap<>();
                            for (CanalEntry.Column column : afterColumnsList) {
                                String key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                                map.put(key, column.getValue());
                            }
                            System.out.println(tableName + " , " + JSON.toJSONString(map));
                        }
                    } else if (eventType == CanalEntry.EventType.DELETE) {
                        // todo... 去完成对应的delete操作的业务逻辑即可
                    }
                }
            }
         }
    }
}